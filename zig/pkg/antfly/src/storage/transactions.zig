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

//! MVCC transaction manager with optimistic concurrency control.
//!
//! Matches Go antfly's db.go transaction system:
//!   - Write intents stored at `\x00\x00__txn_intents__:<txnID>:<key>`
//!   - Transaction records at `\x00\x00__txn_records__:<txnID>`
//!   - Version predicates for conflict detection
//!   - Commit resolves intents → real keys; abort deletes intents

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const docstore = @import("docstore.zig");
const DocStore = docstore.DocStore;
const internal_keys = @import("internal_keys.zig");
const lsm_backend = @import("lsm_backend.zig");
const lmdb = @import("lmdb.zig");
const mem_backend = @import("mem_backend.zig");
const platform_time = @import("antfly_platform").time;
const build_options = @import("build_options");
const tracing = @import("../tracing/antfly_trace_writer.zig");
const stderr_writer = @import("../tracing/stderr_writer.zig");
const ttl = @import("ttl.zig");

// ============================================================================
// Key prefixes
// ============================================================================

const intents_prefix = "\x00\x00__txn_intents__:";
// Key-oriented companion index for O(1) conflict checks on ordinary writes.
// The value is the owning transaction ID. It is installed and removed in the
// same backend batch as the corresponding intent, so the DB apply mutex makes
// lock acquisition and ordinary batch admission linearizable.
const intent_locks_prefix = "\x00\x00__txn_intent_locks__:";
const records_prefix = "\x00\x00__txn_records__:";
const participants_prefix = "\x00\x00__txn_participants__:";
const resolved_participants_prefix = "\x00\x00__txn_resolved_participants__:";
const ha_batch_outbox_prefix = "\x00\x00__txn_ha_batch_outbox__:";
const ha_replay_outbox_prefix = "\x00\x00__txn_ha_replay_outbox__:";

// ============================================================================
// Types
// ============================================================================

pub const TxnId = [16]u8;

pub const TxnStatus = enum(u8) {
    pending = 0,
    committed = 1,
    aborted = 2,
};

pub const WriteIntent = struct {
    key: []const u8,
    value: ?[]const u8, // null for deletes
};

pub const OwnedIntentMutation = struct {
    key: []u8,
    value: ?[]u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.key);
        if (self.value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const PendingIntentInfo = struct {
    txn_id: TxnId,
    value: ?[]u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const VersionPredicate = struct {
    key: []const u8,
    expected_version: u64, // 0 = key must not exist
};

pub const TxnError = error{
    VersionConflict,
    IntentConflict,
    DecisionConflict,
    TxnNotFound,
    InvalidTxnRecord,
};

pub const RecoveryStats = struct {
    scanned_records: u64 = 0,
    auto_aborted: u64 = 0,
    resolved_finalized: u64 = 0,
    cleaned_records: u64 = 0,
    kept_recent_pending: u64 = 0,
    deferred_unresolved: u64 = 0,
};

pub const RecoveryOptions = struct {
    /// Distributed decisions must be replicated by their coordinator. DB-local
    /// maintenance disables this and asks the coordinator callback to propose
    /// the decision through data Raft instead.
    presume_abort_distributed: bool = true,
    /// Retained terminal decisions (transaction sessions/idempotency keys)
    /// use this older cutoff instead of the ordinary recovery cutoff.
    retained_cutoff_timestamp: ?u64 = null,
};

pub const TxnSummary = struct {
    txn_id: TxnId,
    status: TxnStatus,
    begin_timestamp: u64,
    commit_version: u64,
    created_at: u64,
    finalized_at: u64,
    prepared: bool,
    prepared_known: bool,
    coordinator: bool,
    coordinator_known: bool,
    retain_terminal: bool = false,
};

pub const TxnSummaryPage = struct {
    items: []TxnSummary,
    next_after: ?TxnId,
};

pub const ResolutionExtraBatch = struct {
    writes: []const docstore.KVPair = &.{},
    deletes: []const []const u8 = &.{},
    skip_intent_keys: []const []const u8 = &.{},
    replay: ?ReplayAppend = null,
    expected_intent_revision: ?u64 = null,
};

pub const ResolutionOutcome = struct {
    applied: bool,
    replay_sequence: u64,
};

pub const IntentSnapshotValidation = struct {
    status: TxnStatus,
    has_intents: bool,
    replay_sequence: u64,
};

pub const HAOutbox = struct {
    batch_payload: ?[]u8 = null,
    replay_payload: ?[]u8 = null,

    pub fn deinit(self: *HAOutbox, alloc: Allocator) void {
        if (self.batch_payload) |payload| alloc.free(payload);
        if (self.replay_payload) |payload| alloc.free(payload);
        self.* = undefined;
    }
};

pub const HAOutboxKind = enum { batch, replay };

pub const ReplayAppend = struct {
    sequence: u64,
    payload: []const u8,
};

pub const IntentBatch = struct {
    writes: []docstore.KVPair = &.{},
    deletes: [][]const u8 = &.{},
    revision: u64 = 0,

    pub fn deinit(self: *IntentBatch, alloc: Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes) |key| alloc.free(@constCast(key));
        if (self.deletes.len > 0) alloc.free(self.deletes);
        self.* = undefined;
    }
};

const TxnRecord = struct {
    status: TxnStatus,
    begin_timestamp: u64,
    commit_version: u64,
    created_at: u64,
    finalized_at: u64,
    intent_revision: u64 = 0,
    replay_sequence: u64 = 0,
    /// True once this participant has durably accepted its write set.  This is
    /// deliberately separate from `status`: the public protocol continues to
    /// report a prepared participant as pending until a terminal decision is
    /// learned, but recovery must never presume-abort a participant that has
    /// already voted to commit.
    prepared: bool = false,
    /// Older encodings did not persist the prepare vote. Recovery treats
    /// distributed records from those versions conservatively rather than
    /// risking an incorrect abort during a rolling upgrade.
    prepared_known: bool = true,
    /// Only this participant may recover a missing terminal decision by
    /// choosing abort. Other prepared participants must wait for that decision.
    coordinator: bool = false,
    /// Old records predate explicit coordinator ownership and therefore use
    /// conservative recovery semantics during rolling upgrades.
    coordinator_known: bool = true,
    /// Keep the terminal decision for the externally addressable retry window.
    retain_terminal: bool = false,
    /// Old records predate explicit retry retention and may learn it from an
    /// idempotent begin during a rolling upgrade.
    retain_terminal_known: bool = true,

    fn visibleVersion(self: TxnRecord) u64 {
        if (self.commit_version > 0) return self.commit_version;
        return self.begin_timestamp;
    }
};

const txn_record_v0_size = 17;
const txn_record_v1_size = 33;
const txn_record_v2_size = 49;
const txn_record_v3_size = 50;
const txn_record_v4_size = 51;
const txn_record_v5_size = 52;

// ============================================================================
// TxnManager
// ============================================================================

pub const TxnManager = struct {
    store: backend_erased.Store,
    owns_store: bool,
    alloc: Allocator,
    trace_writer: ?tracing.AntflyTraceWriter = null,
    shard_id: []const u8 = "local",

    pub const RecoveryExtraBatchHooks = struct {
        ctx: ?*anyopaque = null,
        build: ?*const fn (
            ctx: ?*anyopaque,
            manager: *TxnManager,
            txn_id: TxnId,
            status: TxnStatus,
            timestamp: u64,
        ) anyerror!ResolutionExtraBatch = null,
        cleanup: ?*const fn (ctx: ?*anyopaque, batch: ResolutionExtraBatch) void = null,
    };

    pub fn init(alloc: Allocator, store: anytype) !TxnManager {
        const runtime_store = try initRuntimeStore(alloc, store);
        return .{
            .alloc = alloc,
            .store = runtime_store.store,
            .owns_store = runtime_store.owned,
            .trace_writer = if (comptime build_options.with_tla) stderr_writer.stderrAntflyTraceWriter() else null,
        };
    }

    pub fn deinit(self: *TxnManager) void {
        if (self.owns_store) self.store.deinit();
        self.* = undefined;
    }

    /// Create a new pending transaction record.
    pub fn initTransaction(self: *TxnManager, txn_id: TxnId, timestamp: u64) !void {
        try self.initTransactionWithParticipants(txn_id, timestamp, &.{});
    }

    pub fn initTransactionWithParticipants(self: *TxnManager, txn_id: TxnId, timestamp: u64, participants: []const []const u8) !void {
        try self.initTransactionWithParticipantsCreatedAt(txn_id, timestamp, timestamp, participants);
    }

    pub fn initTransactionWithParticipantsCreatedAt(
        self: *TxnManager,
        txn_id: TxnId,
        timestamp: u64,
        created_at: u64,
        participants: []const []const u8,
    ) !void {
        return try self.initTransactionWithParticipantsCreatedAtAndRole(txn_id, timestamp, created_at, participants, false);
    }

    pub fn initTransactionWithParticipantsCreatedAtAndRole(
        self: *TxnManager,
        txn_id: TxnId,
        timestamp: u64,
        created_at: u64,
        participants: []const []const u8,
        coordinator: bool,
    ) !void {
        return try self.initTransactionWithParticipantsCreatedAtRoleAndRetention(
            txn_id,
            timestamp,
            created_at,
            participants,
            coordinator,
            false,
        );
    }

    pub fn initTransactionWithParticipantsCreatedAtRoleAndRetention(
        self: *TxnManager,
        txn_id: TxnId,
        timestamp: u64,
        created_at: u64,
        participants: []const []const u8,
        coordinator: bool,
        retain_terminal: bool,
    ) !void {
        const key = makeRecordKey(txn_id);
        const existing = self.loadTransactionRecord(txn_id) catch |err| switch (err) {
            TxnError.TxnNotFound => null,
            else => return err,
        };
        if (existing) |record| {
            if (record.status != .pending or record.begin_timestamp != timestamp) return TxnError.DecisionConflict;
            if (record.coordinator_known and record.coordinator != coordinator) return TxnError.DecisionConflict;
            if (record.retain_terminal_known and record.retain_terminal != retain_terminal) return TxnError.DecisionConflict;
            const persisted = try self.getParticipants(self.alloc, txn_id);
            defer freeParticipantList(self.alloc, persisted);
            if (!participantListsEqual(persisted, participants)) return TxnError.DecisionConflict;
            // An idempotent begin from a new binary is the authoritative point
            // where a legacy record can safely learn its coordinator role.
            // Persist the upgrade before prepare can rewrite the record as v4.
            if (!record.coordinator_known or !record.retain_terminal_known) {
                var upgraded = record;
                upgraded.coordinator = coordinator;
                upgraded.coordinator_known = true;
                upgraded.retain_terminal = retain_terminal;
                upgraded.retain_terminal_known = true;
                try self.saveTransactionRecord(key, upgraded);
            }
            return;
        }
        const record = TxnRecord{
            .status = .pending,
            .begin_timestamp = timestamp,
            .commit_version = 0,
            .created_at = created_at,
            .finalized_at = 0,
            .coordinator = coordinator,
            .retain_terminal = retain_terminal,
        };
        const record_value = try self.encodeRecord(record);
        defer self.alloc.free(record_value);
        const participant_key = makeSidecarKey(participants_prefix, txn_id);
        const resolved_key = makeSidecarKey(resolved_participants_prefix, txn_id);
        const participant_value = if (participants.len > 0) try encodeParticipantList(self.alloc, participants) else null;
        defer if (participant_value) |value| self.alloc.free(value);
        var writes: [2]docstore.KVPair = undefined;
        writes[0] = .{ .key = &key, .value = record_value };
        var write_count: usize = 1;
        if (participant_value) |value| {
            writes[1] = .{ .key = &participant_key, .value = value };
            write_count += 1;
        }
        const deletes: []const []const u8 = if (participants.len > 0)
            &.{&resolved_key}
        else
            &.{ &participant_key, &resolved_key };
        try self.applyBatch(writes[0..write_count], deletes, null);
        if (self.trace_writer) |tw| {
            tw.traceEvent(&.{
                .name = "InitTransaction",
                .txn_id = txn_id,
                .shard_id = self.shard_id,
                .timestamp = timestamp,
            });
        }
    }

    /// Write intents for a transaction, checking version predicates first.
    pub fn writeIntents(
        self: *TxnManager,
        txn_id: TxnId,
        intents: []const WriteIntent,
        predicates: []const VersionPredicate,
    ) !void {
        var record = try self.loadTransactionRecord(txn_id);
        if (record.status != .pending) return TxnError.DecisionConflict;

        // Emit CheckPredicates before checks: TLA+ spec models this as an
        // always-succeeding snapshot step; WriteIntentFails detects conflicts.
        if (self.trace_writer) |tw| {
            tw.traceEvent(&.{
                .name = "CheckPredicates",
                .txn_id = txn_id,
                .shard_id = self.shard_id,
            });
        }

        self.checkVersionPredicates(predicates, txn_id) catch |err| {
            self.traceWriteIntentFails(txn_id, intents, "VersionConflict");
            return err;
        };
        self.checkIntentConflicts(intents, txn_id) catch |err| {
            self.traceWriteIntentFails(txn_id, intents, "IntentConflict");
            return err;
        };

        // Write all intents — collect keys and values, free after putBatch
        var write_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (write_keys.items) |k| self.alloc.free(k);
            write_keys.deinit(self.alloc);
        }
        var write_vals = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (write_vals.items) |v| self.alloc.free(v);
            write_vals.deinit(self.alloc);
        }
        var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
        defer writes.deinit(self.alloc);

        for (intents) |intent| {
            const intent_key = try self.makeIntentKey(txn_id, intent.key);
            try write_keys.append(self.alloc, intent_key);

            // Intent value: [is_delete:u8][value_bytes]
            var val: []u8 = undefined;
            if (intent.value) |v| {
                val = try self.alloc.alloc(u8, 1 + v.len);
                val[0] = 0; // not a delete
                @memcpy(val[1..], v);
            } else {
                val = try self.alloc.alloc(u8, 1);
                val[0] = 1; // is a delete
            }
            try write_vals.append(self.alloc, val);

            try writes.append(self.alloc, .{ .key = intent_key, .value = val });

            const lock_key = try self.makeIntentLockKey(intent.key);
            try write_keys.append(self.alloc, lock_key);
            const owner = try self.alloc.dupe(u8, &txn_id);
            try write_vals.append(self.alloc, owner);
            try writes.append(self.alloc, .{ .key = lock_key, .value = owner });
        }

        record.intent_revision = std.math.add(u64, record.intent_revision, 1) catch return error.TransactionRevisionOverflow;
        // Publish the prepare vote in the same backend batch as the intents.
        // Recovery can therefore never observe intents without the durable
        // fencing bit that prevents presumed abort.
        record.prepared = true;
        const record_key = makeRecordKey(txn_id);
        const record_value = try self.encodeRecord(record);
        try write_vals.append(self.alloc, record_value);
        try writes.append(self.alloc, .{ .key = &record_key, .value = record_value });

        try self.applyBatch(writes.items, &.{}, null);

        self.traceWriteIntentSuccess(txn_id, intents, predicates);
    }

    /// Resolve intents: commit applies them to real keys, abort deletes them.
    /// Conflicting terminal decisions return `TxnError.DecisionConflict`; callers
    /// should treat that as a protocol inconsistency / torn-state signal rather
    /// than a retryable OCC conflict.
    pub fn resolveIntents(self: *TxnManager, txn_id: TxnId, status: TxnStatus, timestamp: u64) !void {
        _ = try self.resolveIntentsWithExtraBatch(txn_id, status, timestamp, .{});
    }

    pub fn resolveIntentsWithExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        status: TxnStatus,
        timestamp: u64,
        extra_batch: ResolutionExtraBatch,
    ) !ResolutionOutcome {
        const rec_key = makeRecordKey(txn_id);
        var record = try self.loadTransactionRecord(txn_id);
        if (extra_batch.expected_intent_revision) |expected| {
            if (record.intent_revision != expected) return error.IntentSnapshotChanged;
        }
        const was_terminal = record.status != .pending;
        applyResolveDecision(&record, status, timestamp) catch |err| {
            if (err == TxnError.DecisionConflict) {
                if (self.trace_writer) |tw| {
                    tw.traceEvent(&.{
                        .name = "ResolveDecisionConflict",
                        .txn_id = txn_id,
                        .shard_id = self.shard_id,
                        .timestamp = timestamp,
                        .reason = resolveDecisionConflictReason(record.status, status),
                    });
                }
            }
            return err;
        };

        // Scan all intents for this txn
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        const intent_entries = try self.scanPrefix(self.alloc, scan_prefix);
        defer backend_scan.freeResults(self.alloc, intent_entries);

        // A resolve retry after the terminal record and all intents are already
        // durable must not apply the caller's derived batch again. In
        // particular, doing so could overwrite a newer user write with the
        // transaction's old value.
        if (was_terminal and intent_entries.len == 0) return .{
            .applied = false,
            .replay_sequence = record.replay_sequence,
        };

        var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
        defer writes.deinit(self.alloc);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);
        var owned_apply_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_apply_keys.items) |key| self.alloc.free(key);
            owned_apply_keys.deinit(self.alloc);
        }

        // Always delete the intent keys
        for (intent_entries) |entry| {
            try deletes.append(self.alloc, entry.key);
            const user_key = entry.key[intents_prefix.len + 17 ..];
            const lock_key = try self.makeIntentLockKey(user_key);
            try owned_apply_keys.append(self.alloc, lock_key);
            try deletes.append(self.alloc, lock_key);
        }

        if (status == .committed) {
            // Apply intents to real keys
            for (intent_entries) |entry| {
                // Extract user key from intent key:
                // intents_prefix(20) + txn_id(16) + ':'(1) + user_key
                const user_key = entry.key[intents_prefix.len + 17 ..];
                if (containsKey(extra_batch.skip_intent_keys, user_key)) continue;

                if (internal_keys.isInternalPhysicalTableDataKey(user_key)) {
                    if (entry.value.len > 0 and entry.value[0] == 1) {
                        try deletes.append(self.alloc, user_key);
                    } else {
                        const val = if (entry.value.len > 1) entry.value[1..] else "";
                        try writes.append(self.alloc, .{ .key = user_key, .value = val });
                    }
                    continue;
                }

                if (entry.value.len > 0 and entry.value[0] == 1) {
                    // Delete — also remove the timestamp entry
                    const store_key = try internal_keys.documentKeyAlloc(self.alloc, user_key);
                    try owned_apply_keys.append(self.alloc, store_key);
                    try deletes.append(self.alloc, store_key);

                    const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, user_key);
                    try owned_apply_keys.append(self.alloc, ts_key);
                    try deletes.append(self.alloc, ts_key);
                } else {
                    // Put
                    const val = if (entry.value.len > 1) entry.value[1..] else "";
                    const store_key = try internal_keys.documentKeyAlloc(self.alloc, user_key);
                    try owned_apply_keys.append(self.alloc, store_key);
                    try writes.append(self.alloc, .{ .key = store_key, .value = val });

                    // Write timestamp for the key
                    const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, user_key);
                    try owned_apply_keys.append(self.alloc, ts_key);
                    var ts_val: [8]u8 = undefined;
                    std.mem.writeInt(u64, &ts_val, timestamp, .little);
                    try writes.append(self.alloc, .{ .key = ts_key, .value = &ts_val });
                }
            }
        }

        if (status == .committed) {
            if (extra_batch.replay) |replay| record.replay_sequence = replay.sequence;
        }
        const rec_val = try self.encodeRecord(record);
        defer self.alloc.free(rec_val);
        try writes.append(self.alloc, .{ .key = &rec_key, .value = rec_val });
        try writes.appendSlice(self.alloc, extra_batch.writes);
        try deletes.appendSlice(self.alloc, extra_batch.deletes);

        try self.applyBatch(writes.items, deletes.items, extra_batch.replay);

        if (self.trace_writer) |tw| {
            tw.traceEvent(&.{
                .name = if (status == .committed) "CommitTransaction" else "AbortTransaction",
                .txn_id = txn_id,
                .shard_id = self.shard_id,
            });
            tw.traceEvent(&.{
                .name = "ResolveIntentsOnShard",
                .txn_id = txn_id,
                .shard_id = self.shard_id,
                .timestamp = timestamp,
                .reason = if (status == .committed) "committed" else "aborted",
            });
        }
        return .{
            .applied = true,
            .replay_sequence = record.replay_sequence,
        };
    }

    pub fn collectIntentBatch(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !IntentBatch {
        // Read the revision before scanning. Intent writes publish the record
        // revision and intent rows in one backend batch, so validation under
        // the DB apply lock detects any prepare that raced with this snapshot.
        const record = try self.loadTransactionRecord(txn_id);
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        const intent_entries = try self.scanPrefix(alloc, scan_prefix);
        defer backend_scan.freeResults(alloc, intent_entries);

        var write_count: usize = 0;
        for (intent_entries) |entry| {
            if (!(entry.value.len > 0 and entry.value[0] == 1)) write_count += 1;
        }
        const writes = try alloc.alloc(docstore.KVPair, write_count);
        var writes_initialized: usize = 0;
        errdefer {
            for (writes[0..writes_initialized]) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            if (writes.len > 0) alloc.free(writes);
        }
        const deletes = try alloc.alloc([]const u8, intent_entries.len - write_count);
        var deletes_initialized: usize = 0;
        errdefer {
            for (deletes[0..deletes_initialized]) |key| alloc.free(@constCast(key));
            if (deletes.len > 0) alloc.free(deletes);
        }

        for (intent_entries) |entry| {
            const user_key = entry.key[intents_prefix.len + 17 ..];
            if (entry.value.len > 0 and entry.value[0] == 1) {
                deletes[deletes_initialized] = try alloc.dupe(u8, user_key);
                deletes_initialized += 1;
            } else {
                const key = try alloc.dupe(u8, user_key);
                errdefer alloc.free(key);
                const value = try alloc.dupe(u8, if (entry.value.len > 1) entry.value[1..] else "");
                writes[writes_initialized] = .{ .key = key, .value = value };
                writes_initialized += 1;
            }
        }
        return .{ .writes = writes, .deletes = deletes, .revision = record.intent_revision };
    }

    pub fn hasIntents(self: *TxnManager, txn_id: TxnId) !bool {
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        var read = try self.store.beginRead();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const entry = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        return std.mem.startsWith(u8, entry.key, prefix);
    }

    pub fn validateIntentSnapshot(self: *TxnManager, txn_id: TxnId, expected_revision: u64) !IntentSnapshotValidation {
        const record = try self.loadTransactionRecord(txn_id);
        if (record.intent_revision != expected_revision) return error.IntentSnapshotChanged;
        return .{
            .status = record.status,
            .has_intents = try self.hasIntents(txn_id),
            .replay_sequence = record.replay_sequence,
        };
    }

    pub fn loadHAOutbox(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !HAOutbox {
        const batch_key = makeTransactionHABatchOutboxKey(txn_id);
        const replay_key = makeTransactionHAReplayOutboxKey(txn_id);
        var out: HAOutbox = .{};
        errdefer out.deinit(alloc);
        out.batch_payload = self.getAlloc(alloc, &batch_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        out.replay_payload = self.getAlloc(alloc, &replay_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        return out;
    }

    pub fn hasHAOutbox(self: *TxnManager, txn_id: TxnId) !bool {
        const batch_key = makeTransactionHABatchOutboxKey(txn_id);
        const replay_key = makeTransactionHAReplayOutboxKey(txn_id);
        return try self.keyExists(&batch_key) or try self.keyExists(&replay_key);
    }

    pub fn clearHAOutbox(self: *TxnManager, txn_id: TxnId, kind: HAOutboxKind) !void {
        switch (kind) {
            .batch => {
                const key = makeTransactionHABatchOutboxKey(txn_id);
                try self.applyBatch(&.{}, &.{&key}, null);
            },
            .replay => {
                const key = makeTransactionHAReplayOutboxKey(txn_id);
                try self.applyBatch(&.{}, &.{&key}, null);
            },
        }
    }

    pub fn collectIntentDocumentKeys(
        self: *TxnManager,
        alloc: Allocator,
        txn_id: TxnId,
        upserts: *std.ArrayListUnmanaged([]const u8),
        deletes: *std.ArrayListUnmanaged([]const u8),
    ) !void {
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        const intent_entries = try self.scanPrefix(alloc, scan_prefix);
        defer backend_scan.freeResults(alloc, intent_entries);

        for (intent_entries) |entry| {
            const user_key = entry.key[intents_prefix.len + 17 ..];
            const owned_key = try alloc.dupe(u8, user_key);
            if (entry.value.len > 0 and entry.value[0] == 1) {
                try deletes.append(alloc, owned_key);
            } else {
                try upserts.append(alloc, owned_key);
            }
        }
    }

    pub fn collectIntentMutations(self: *TxnManager, alloc: Allocator, txn_id: TxnId) ![]OwnedIntentMutation {
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        const intent_entries = try self.scanPrefix(alloc, scan_prefix);
        defer backend_scan.freeResults(alloc, intent_entries);

        var out = std.ArrayListUnmanaged(OwnedIntentMutation).empty;
        errdefer {
            for (out.items) |*item| item.deinit(alloc);
            out.deinit(alloc);
        }

        for (intent_entries) |entry| {
            const user_key = entry.key[intents_prefix.len + 17 ..];
            const owned_key = try alloc.dupe(u8, user_key);
            var owned_key_pending = true;
            errdefer if (owned_key_pending) alloc.free(owned_key);
            const owned_value = if (entry.value.len > 0 and entry.value[0] == 1)
                null
            else
                try alloc.dupe(u8, if (entry.value.len > 1) entry.value[1..] else "");
            var owned_value_pending = owned_value != null;
            errdefer if (owned_value_pending) {
                if (owned_value) |value| alloc.free(value);
            };
            try out.append(alloc, .{
                .key = owned_key,
                .value = owned_value,
            });
            owned_key_pending = false;
            owned_value_pending = false;
        }

        return try out.toOwnedSlice(alloc);
    }

    pub fn collectPendingIntentsForKeyAlloc(
        self: *TxnManager,
        alloc: Allocator,
        user_key: []const u8,
        exclude_txn: ?TxnId,
    ) ![]PendingIntentInfo {
        const all_intents = try self.scanPrefix(alloc, intents_prefix);
        defer backend_scan.freeResults(alloc, all_intents);

        var out = std.ArrayListUnmanaged(PendingIntentInfo).empty;
        errdefer {
            for (out.items) |*item| item.deinit(alloc);
            out.deinit(alloc);
        }

        for (all_intents) |entry| {
            if (entry.key.len < intents_prefix.len + 17) continue;
            const entry_txn_id = entry.key[intents_prefix.len..][0..16].*;
            if (exclude_txn) |txn_id| {
                if (std.mem.eql(u8, &entry_txn_id, &txn_id)) continue;
            }
            const entry_user_key = entry.key[intents_prefix.len + 17 ..];
            if (!std.mem.eql(u8, entry_user_key, user_key)) continue;

            const status = self.getTransactionStatus(entry_txn_id) catch continue;
            if (status != .pending) continue;

            const owned_value = if (entry.value.len > 0 and entry.value[0] == 1)
                null
            else
                try alloc.dupe(u8, if (entry.value.len > 1) entry.value[1..] else "");
            var owned_value_pending = owned_value != null;
            errdefer if (owned_value_pending) {
                if (owned_value) |value| alloc.free(value);
            };
            try out.append(alloc, .{
                .txn_id = entry_txn_id,
                .value = owned_value,
            });
            owned_value_pending = false;
        }

        return try out.toOwnedSlice(alloc);
    }

    /// Get the status of a transaction.
    pub fn getTransactionStatus(self: *TxnManager, txn_id: TxnId) !TxnStatus {
        return (try self.loadTransactionRecord(txn_id)).status;
    }

    pub fn getCommitVersion(self: *TxnManager, txn_id: TxnId) !u64 {
        return (try self.loadTransactionRecord(txn_id)).visibleVersion();
    }

    /// Stable API sessions keep the coordinator's self-acknowledgement pending
    /// until their terminal response is durable in the session registry.
    pub fn defersCoordinatorAcknowledgement(self: *TxnManager, txn_id: TxnId) !bool {
        const record = try self.loadTransactionRecord(txn_id);
        return record.coordinator and record.retain_terminal and record.status == .committed;
    }

    pub fn listTransactions(self: *TxnManager, alloc: Allocator) ![]TxnSummary {
        return (try self.listTransactionsPage(alloc, null, std.math.maxInt(usize))).items;
    }

    /// Returns whether a topology transition would strand transaction state.
    /// This stays allocation-free and uses a single backend read snapshot:
    /// transitions are cold-path operations, but retained terminal decisions
    /// can make the transaction history large.
    pub fn hasTopologySensitiveTransactions(self: *TxnManager) !bool {
        var read = try self.store.beginRead();
        defer read.abort();

        // Resolution normally removes intents and HA outboxes atomically before
        // a transaction becomes quiescent. Check the global prefixes once rather
        // than opening an intent cursor for every retained terminal record.
        if (try readHasPrefix(&read, intents_prefix) or
            try readHasPrefix(&read, intent_locks_prefix) or
            try readHasPrefix(&read, ha_batch_outbox_prefix) or
            try readHasPrefix(&read, ha_replay_outbox_prefix))
        {
            return true;
        }

        var cursor = try read.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(records_prefix);
        while (entry) |record_entry| {
            if (!std.mem.startsWith(u8, record_entry.key, records_prefix)) break;
            if (record_entry.key.len == records_prefix.len + 16) {
                const record = try decodeRecord(record_entry.value);
                if (record.status == .pending) return true;

                const txn_id = record_entry.key[records_prefix.len..][0..16].*;
                const participant_key = makeSidecarKey(participants_prefix, txn_id);
                const resolved_key = makeSidecarKey(resolved_participants_prefix, txn_id);
                const participant_count = try participantListCountInRead(&read, &participant_key);
                const resolved_count = try participantListCountInRead(&read, &resolved_key);
                if (resolved_count > participant_count) return TxnError.InvalidTxnRecord;
                // markParticipantResolved only appends unique, enlisted members,
                // so equal validated counts prove that the participant set is
                // fully acknowledged without allocating or comparing strings.
                if (resolved_count != participant_count) return true;
            }
            entry = try cursor.next();
        }
        return false;
    }

    pub fn listTransactionsPage(
        self: *TxnManager,
        alloc: Allocator,
        after: ?TxnId,
        limit: usize,
    ) !TxnSummaryPage {
        if (limit == 0) return .{ .items = try alloc.alloc(TxnSummary, 0), .next_after = after };
        var items = std.ArrayListUnmanaged(TxnSummary).empty;
        errdefer items.deinit(alloc);
        var read = try self.store.beginRead();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        var entry = if (after) |txn_id| blk: {
            const key = makeRecordKey(txn_id);
            const found = (try cursor.seekAtOrAfter(&key)) orelse break :blk null;
            break :blk if (std.mem.eql(u8, found.key, &key)) try cursor.next() else found;
        } else try cursor.seekAtOrAfter(records_prefix);
        var last: ?TxnId = null;
        while (entry) |record_entry| {
            if (!std.mem.startsWith(u8, record_entry.key, records_prefix)) break;
            if (record_entry.key.len != records_prefix.len + 16) {
                entry = try cursor.next();
                continue;
            }
            const record = try decodeRecord(record_entry.value);
            const txn_id = record_entry.key[records_prefix.len..][0..16].*;
            try items.append(alloc, .{
                .txn_id = txn_id,
                .status = record.status,
                .begin_timestamp = record.begin_timestamp,
                .commit_version = record.commit_version,
                .created_at = record.created_at,
                .finalized_at = record.finalized_at,
                .prepared = record.prepared,
                .prepared_known = record.prepared_known,
                .coordinator = record.coordinator,
                .coordinator_known = record.coordinator_known,
                .retain_terminal = record.retain_terminal,
            });
            last = txn_id;
            entry = try cursor.next();
            if (items.items.len >= limit) break;
        }
        const has_more = if (entry) |next| std.mem.startsWith(u8, next.key, records_prefix) else false;
        return .{
            .items = try items.toOwnedSlice(alloc),
            .next_after = if (has_more) last else null,
        };
    }

    pub fn markParticipantResolved(self: *TxnManager, txn_id: TxnId, participant: []const u8) !void {
        // A replicated acknowledgement can be retried after the coordinator
        // has already cleaned the transaction. Do not recreate an orphaned
        // resolved-participants sidecar in that case, and reject corrupt
        // acknowledgements for participants that were never enlisted.
        _ = try self.loadTransactionRecord(txn_id);
        const participants = try self.getParticipants(self.alloc, txn_id);
        defer freeParticipantList(self.alloc, participants);
        var enlisted = false;
        for (participants) |existing| {
            if (std.mem.eql(u8, existing, participant)) {
                enlisted = true;
                break;
            }
        }
        if (!enlisted) return error.InvalidParticipant;

        const resolved = try self.getResolvedParticipants(self.alloc, txn_id);
        defer freeParticipantList(self.alloc, resolved);

        for (resolved) |existing| {
            if (std.mem.eql(u8, existing, participant)) return;
        }

        var next = try self.alloc.alloc([]u8, resolved.len + 1);
        var initialized: usize = 0;
        errdefer {
            for (next[0..initialized]) |entry| self.alloc.free(entry);
            self.alloc.free(next);
        }
        for (resolved, 0..) |existing, i| {
            next[i] = try self.alloc.dupe(u8, existing);
            initialized += 1;
        }
        next[resolved.len] = try self.alloc.dupe(u8, participant);
        initialized += 1;
        defer freeParticipantList(self.alloc, next);
        try self.saveOwnedParticipantSet(resolved_participants_prefix, txn_id, next);
    }

    pub fn getParticipants(self: *TxnManager, alloc: Allocator, txn_id: TxnId) ![][]u8 {
        return try self.loadParticipantSet(alloc, participants_prefix, txn_id);
    }

    pub fn getResolvedParticipants(self: *TxnManager, alloc: Allocator, txn_id: TxnId) ![][]u8 {
        return try self.loadParticipantSet(alloc, resolved_participants_prefix, txn_id);
    }

    pub fn getUnresolvedParticipants(self: *TxnManager, alloc: Allocator, txn_id: TxnId) ![][]u8 {
        const participants = try self.getParticipants(alloc, txn_id);
        errdefer freeParticipantList(alloc, participants);
        const resolved = try self.getResolvedParticipants(alloc, txn_id);
        defer freeParticipantList(alloc, resolved);

        var unresolved = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (unresolved.items) |entry| alloc.free(entry);
            unresolved.deinit(alloc);
        }

        outer: for (participants) |participant| {
            for (resolved) |done| {
                if (std.mem.eql(u8, participant, done)) continue :outer;
            }
            try unresolved.append(alloc, try alloc.dupe(u8, participant));
        }

        freeParticipantList(alloc, participants);
        return try unresolved.toOwnedSlice(alloc);
    }

    pub fn recoverTransactions(self: *TxnManager, cutoff_timestamp: u64, resolution_timestamp: u64) !RecoveryStats {
        return try self.recoverTransactionsWithExtraBatchHooks(cutoff_timestamp, resolution_timestamp, .{});
    }

    pub fn recoverTransactionsWithExtraBatchHooks(
        self: *TxnManager,
        cutoff_timestamp: u64,
        resolution_timestamp: u64,
        extra_hooks: RecoveryExtraBatchHooks,
    ) !RecoveryStats {
        return try self.recoverTransactionsWithExtraBatchHooksAndOptions(
            cutoff_timestamp,
            resolution_timestamp,
            extra_hooks,
            .{},
        );
    }

    pub fn recoverTransactionsWithExtraBatchHooksAndOptions(
        self: *TxnManager,
        cutoff_timestamp: u64,
        resolution_timestamp: u64,
        extra_hooks: RecoveryExtraBatchHooks,
        options: RecoveryOptions,
    ) !RecoveryStats {
        const summaries = try self.listTransactions(self.alloc);
        defer self.alloc.free(summaries);
        return try self.recoverTransactionSummariesWithExtraBatchHooksAndOptions(
            summaries,
            cutoff_timestamp,
            resolution_timestamp,
            extra_hooks,
            options,
        );
    }

    pub fn recoverTransactionSummariesWithExtraBatchHooksAndOptions(
        self: *TxnManager,
        summaries: []const TxnSummary,
        cutoff_timestamp: u64,
        resolution_timestamp: u64,
        extra_hooks: RecoveryExtraBatchHooks,
        options: RecoveryOptions,
    ) !RecoveryStats {
        var stats: RecoveryStats = .{};
        for (summaries) |summary| {
            const txn_id = summary.txn_id;
            stats.scanned_records += 1;

            if (summary.status == .pending) {
                if (summary.created_at > 0 and summary.created_at < cutoff_timestamp) {
                    if (!options.presume_abort_distributed) {
                        const participants = try self.getParticipants(self.alloc, txn_id);
                        defer freeParticipantList(self.alloc, participants);
                        if (participants.len > 0) {
                            stats.kept_recent_pending += 1;
                            continue;
                        }
                    }
                    if ((summary.prepared or !summary.prepared_known) and (!summary.coordinator or !summary.coordinator_known)) {
                        const participants = try self.getParticipants(self.alloc, txn_id);
                        defer freeParticipantList(self.alloc, participants);
                        if (participants.len > 0) {
                            // A distributed participant that has voted yes is
                            // blocked on the durable coordinator decision. It
                            // is unsafe to infer abort from elapsed time.
                            stats.kept_recent_pending += 1;
                            continue;
                        }
                    }
                    try self.resolveIntents(txn_id, .aborted, resolution_timestamp);
                    stats.auto_aborted += 1;
                    if (self.trace_writer) |tw| {
                        tw.traceEvent(&.{
                            .name = "RecoveryResolve",
                            .txn_id = txn_id,
                            .shard_id = self.shard_id,
                            .reason = "auto-abort-stale",
                        });
                    }
                } else {
                    stats.kept_recent_pending += 1;
                }
                continue;
            }

            if (try self.hasAnyIntents(txn_id)) {
                const resolve_ts = switch (summary.status) {
                    .committed => if (summary.commit_version > 0) summary.commit_version else summary.begin_timestamp,
                    .aborted => if (summary.finalized_at > 0) summary.finalized_at else resolution_timestamp,
                    .pending => unreachable,
                };
                var extra_batch: ResolutionExtraBatch = .{};
                var extra_batch_initialized = false;
                defer if (extra_batch_initialized) {
                    if (extra_hooks.cleanup) |cleanup| cleanup(extra_hooks.ctx, extra_batch);
                };
                if (extra_hooks.build) |build| {
                    extra_batch = try build(extra_hooks.ctx, self, txn_id, summary.status, resolve_ts);
                    extra_batch_initialized = true;
                }
                _ = try self.resolveIntentsWithExtraBatch(txn_id, summary.status, resolve_ts, extra_batch);
                stats.resolved_finalized += 1;
            }

            const unresolved = try self.getUnresolvedParticipants(self.alloc, txn_id);
            defer freeParticipantList(self.alloc, unresolved);
            if (unresolved.len > 0) {
                stats.deferred_unresolved += 1;
                continue;
            }

            const refreshed = try self.loadTransactionRecord(txn_id);
            const cleanup_cutoff = if (refreshed.retain_terminal)
                options.retained_cutoff_timestamp orelse cutoff_timestamp
            else
                cutoff_timestamp;
            if (refreshed.status != .pending and refreshed.finalized_at < cleanup_cutoff and
                !try self.hasAnyIntents(txn_id) and !try self.hasHAOutbox(txn_id))
            {
                try self.deleteTransactionMetadata(txn_id);
                stats.cleaned_records += 1;
                if (self.trace_writer) |tw| {
                    tw.traceEvent(&.{
                        .name = "CleanupTxnRecord",
                        .txn_id = txn_id,
                        .shard_id = self.shard_id,
                    });
                }
            }
        }

        return stats;
    }

    pub fn checkVersionPredicates(
        self: *TxnManager,
        predicates: []const VersionPredicate,
        exclude_txn: ?TxnId,
    ) !void {
        for (predicates) |pred| {
            const current_ts = try self.readTimestamp(pred.key);
            if (pred.expected_version == 0) {
                if (current_ts != null) return TxnError.VersionConflict;
            } else {
                const ts = current_ts orelse return TxnError.VersionConflict;
                if (ts != pred.expected_version) return TxnError.VersionConflict;
            }

            if (try self.hasPendingIntentForKey(pred.key, exclude_txn)) {
                return TxnError.IntentConflict;
            }
        }
    }

    pub fn checkIntentConflicts(
        self: *TxnManager,
        intents: []const WriteIntent,
        exclude_txn: ?TxnId,
    ) !void {
        for (intents) |intent| {
            if (try self.hasPendingIntentForKey(intent.key, exclude_txn)) {
                return TxnError.IntentConflict;
            }
        }
    }

    /// Check if any other pending transaction has an intent on this key.
    fn hasPendingIntentForKey(self: *TxnManager, user_key: []const u8, exclude_txn: ?TxnId) !bool {
        const lock_key = try self.makeIntentLockKey(user_key);
        defer self.alloc.free(lock_key);
        const owner = self.getAlloc(self.alloc, lock_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        defer self.alloc.free(owner);
        if (owner.len != @sizeOf(TxnId)) return TxnError.InvalidTxnRecord;
        if (exclude_txn) |txn_id| if (std.mem.eql(u8, owner, &txn_id)) return false;
        return true;
    }

    pub fn checkOrdinaryWriteConflict(self: *TxnManager, key: []const u8) !void {
        if (try self.hasPendingIntentForKey(key, null)) return TxnError.IntentConflict;
    }

    /// Build an intent key: intents_prefix + txn_id + ':' + user_key
    fn makeIntentKey(self: *TxnManager, txn_id: TxnId, user_key: []const u8) ![]u8 {
        const total = intents_prefix.len + 16 + 1 + user_key.len;
        const key = try self.alloc.alloc(u8, total);
        @memcpy(key[0..intents_prefix.len], intents_prefix);
        @memcpy(key[intents_prefix.len..][0..16], &txn_id);
        key[intents_prefix.len + 16] = ':';
        @memcpy(key[intents_prefix.len + 17 ..], user_key);
        return key;
    }

    fn makeIntentLockKey(self: *TxnManager, user_key: []const u8) ![]u8 {
        const key = try self.alloc.alloc(u8, intent_locks_prefix.len + user_key.len);
        @memcpy(key[0..intent_locks_prefix.len], intent_locks_prefix);
        @memcpy(key[intent_locks_prefix.len..], user_key);
        return key;
    }

    fn loadTransactionRecord(self: *TxnManager, txn_id: TxnId) !TxnRecord {
        const key = makeRecordKey(txn_id);
        const val = self.getAlloc(self.alloc, &key) catch |err| switch (err) {
            error.NotFound => return TxnError.TxnNotFound,
            else => return err,
        };
        defer self.alloc.free(val);
        return try decodeRecord(val);
    }

    fn saveTransactionRecord(self: *TxnManager, key: [records_prefix.len + 16]u8, record: TxnRecord) !void {
        const encoded = try self.encodeRecord(record);
        defer self.alloc.free(encoded);
        try self.putValue(&key, encoded);
    }

    fn encodeRecord(self: *TxnManager, record: TxnRecord) ![]u8 {
        const buf = try self.alloc.alloc(u8, txn_record_v5_size);
        buf[0] = @intFromEnum(record.status);
        std.mem.writeInt(u64, buf[1..9], record.begin_timestamp, .little);
        std.mem.writeInt(u64, buf[9..17], record.commit_version, .little);
        std.mem.writeInt(u64, buf[17..25], record.created_at, .little);
        std.mem.writeInt(u64, buf[25..33], record.finalized_at, .little);
        std.mem.writeInt(u64, buf[33..41], record.intent_revision, .little);
        std.mem.writeInt(u64, buf[41..49], record.replay_sequence, .little);
        buf[49] = @intFromBool(record.prepared);
        buf[50] = @intFromBool(record.coordinator);
        buf[51] = @intFromBool(record.retain_terminal);
        return buf;
    }

    /// Apply the same cleanup predicate on every replicated state-machine
    /// copy. A missing record is already clean and is therefore idempotent.
    pub fn cleanupTransactionMetadataIfEligible(
        self: *TxnManager,
        txn_id: TxnId,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) !bool {
        const record = self.loadTransactionRecord(txn_id) catch |err| switch (err) {
            TxnError.TxnNotFound => return false,
            else => return err,
        };
        if (record.status == .pending or try self.hasAnyIntents(txn_id) or try self.hasHAOutbox(txn_id)) return false;
        const cutoff = if (record.retain_terminal) retained_cutoff_timestamp else cutoff_timestamp;
        if (record.finalized_at >= cutoff) return false;
        const unresolved = try self.getUnresolvedParticipants(self.alloc, txn_id);
        defer freeParticipantList(self.alloc, unresolved);
        if (unresolved.len != 0) return false;
        try self.deleteTransactionMetadata(txn_id);
        return true;
    }

    fn hasAnyIntents(self: *TxnManager, txn_id: TxnId) !bool {
        var prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(prefix_buf[intents_prefix.len..][0..16], &txn_id);
        prefix_buf[intents_prefix.len + 16] = ':';
        const intents = try self.scanPrefix(self.alloc, prefix_buf[0 .. intents_prefix.len + 17]);
        defer backend_scan.freeResults(self.alloc, intents);
        return intents.len > 0;
    }

    fn scanPrefix(self: *TxnManager, alloc: Allocator, prefix: []const u8) ![]backend_scan.OwnedKVPair {
        return try backend_scan.scanPrefix(alloc, &self.store, prefix);
    }

    fn deleteTransactionMetadata(self: *TxnManager, txn_id: TxnId) !void {
        const record_key = makeRecordKey(txn_id);
        const participant_key = makeSidecarKey(participants_prefix, txn_id);
        const resolved_key = makeSidecarKey(resolved_participants_prefix, txn_id);
        const ha_batch_key = makeTransactionHABatchOutboxKey(txn_id);
        const ha_replay_key = makeTransactionHAReplayOutboxKey(txn_id);
        try self.applyBatch(&.{}, &.{ &record_key, &participant_key, &resolved_key, &ha_batch_key, &ha_replay_key }, null);
    }

    fn saveParticipantSet(self: *TxnManager, comptime prefix: []const u8, txn_id: TxnId, participants: []const []const u8) !void {
        var owned = try self.alloc.alloc([]u8, participants.len);
        defer {
            for (owned) |entry| self.alloc.free(entry);
            self.alloc.free(owned);
        }
        for (participants, 0..) |participant, i| {
            owned[i] = try self.alloc.dupe(u8, participant);
        }
        try self.saveOwnedParticipantSet(prefix, txn_id, owned);
    }

    fn saveOwnedParticipantSet(self: *TxnManager, comptime prefix: []const u8, txn_id: TxnId, participants: []const []u8) !void {
        const key = makeSidecarKey(prefix, txn_id);
        if (participants.len == 0) {
            try self.applyBatch(&.{}, &.{&key}, null);
            return;
        }
        const encoded = try encodeParticipantList(self.alloc, participants);
        defer self.alloc.free(encoded);
        try self.putValue(&key, encoded);
    }

    fn loadParticipantSet(self: *TxnManager, alloc: Allocator, comptime prefix: []const u8, txn_id: TxnId) ![][]u8 {
        const key = makeSidecarKey(prefix, txn_id);
        const raw = self.getAlloc(alloc, &key) catch |err| switch (err) {
            error.NotFound => return alloc.alloc([]u8, 0),
            else => return err,
        };
        defer alloc.free(raw);
        return try decodeParticipantList(alloc, raw);
    }

    fn getAlloc(self: *TxnManager, alloc: Allocator, key: []const u8) ![]u8 {
        var txn = try self.store.beginRead();
        defer txn.abort();
        const value = try txn.get(key);
        return try alloc.dupe(u8, value);
    }

    fn keyExists(self: *TxnManager, key: []const u8) !bool {
        var txn = try self.store.beginRead();
        defer txn.abort();
        _ = txn.get(key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        return true;
    }

    fn putValue(self: *TxnManager, key: []const u8, value: []const u8) !void {
        var txn = try self.store.beginWrite();
        errdefer txn.abort();
        try txn.put(key, value);
        try txn.commit();
    }

    fn applyBatch(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend) !void {
        var batch = try self.store.beginBatch();
        errdefer batch.abort();
        for (deletes) |key| {
            batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        for (writes) |kv| {
            try batch.put(kv.key, kv.value);
        }
        if (replay) |entry| try batch.setReplayOpaque(entry.sequence, entry.payload);
        try batch.commit();
    }

    fn traceWriteIntentSuccess(self: *TxnManager, txn_id: TxnId, intents: []const WriteIntent, predicates: []const VersionPredicate) void {
        const tw = self.trace_writer orelse return;
        var write_keys_buf: [32][]const u8 = undefined;
        var delete_keys_buf: [32][]const u8 = undefined;
        var predicate_keys_buf: [32][]const u8 = undefined;
        var wk: usize = 0;
        var dk: usize = 0;
        for (intents) |intent| {
            if (intent.value != null) {
                if (wk < write_keys_buf.len) {
                    write_keys_buf[wk] = intent.key;
                    wk += 1;
                }
            } else {
                if (dk < delete_keys_buf.len) {
                    delete_keys_buf[dk] = intent.key;
                    dk += 1;
                }
            }
        }
        // TLA+ TxnReadSet = predicateKeys; include write keys since
        // checkIntentConflicts checks intents on write keys too.
        var pk: usize = 0;
        for (predicates) |pred| {
            if (pk < predicate_keys_buf.len) {
                predicate_keys_buf[pk] = pred.key;
                pk += 1;
            }
        }
        for (intents) |intent| {
            if (pk < predicate_keys_buf.len) {
                predicate_keys_buf[pk] = intent.key;
                pk += 1;
            }
        }
        tw.traceEvent(&.{
            .name = "WriteIntentOnShard",
            .txn_id = txn_id,
            .shard_id = self.shard_id,
            .write_keys = write_keys_buf[0..wk],
            .delete_keys = delete_keys_buf[0..dk],
            .predicate_keys = predicate_keys_buf[0..pk],
        });
    }

    fn traceWriteIntentFails(self: *TxnManager, txn_id: TxnId, intents: []const WriteIntent, reason: []const u8) void {
        const tw = self.trace_writer orelse return;
        var write_keys_buf: [32][]const u8 = undefined;
        var delete_keys_buf: [32][]const u8 = undefined;
        var predicate_keys_buf: [32][]const u8 = undefined;
        var wk: usize = 0;
        var dk: usize = 0;
        for (intents) |intent| {
            if (intent.value != null) {
                if (wk < write_keys_buf.len) {
                    write_keys_buf[wk] = intent.key;
                    wk += 1;
                }
            } else {
                if (dk < delete_keys_buf.len) {
                    delete_keys_buf[dk] = intent.key;
                    dk += 1;
                }
            }
        }
        // Include write keys as predicateKeys so TLA+ NoConflictingIntents
        // can detect intent conflicts on those keys.
        var pk: usize = 0;
        for (intents) |intent| {
            if (pk < predicate_keys_buf.len) {
                predicate_keys_buf[pk] = intent.key;
                pk += 1;
            }
        }
        tw.traceEvent(&.{
            .name = "WriteIntentFails",
            .txn_id = txn_id,
            .shard_id = self.shard_id,
            .write_keys = write_keys_buf[0..wk],
            .delete_keys = delete_keys_buf[0..dk],
            .predicate_keys = predicate_keys_buf[0..pk],
            .reason = reason,
        });
    }

    fn readTimestamp(self: *TxnManager, key: []const u8) !?u64 {
        const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, key);
        defer self.alloc.free(ts_key);
        const val = self.getAlloc(self.alloc, ts_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(val);
        if (val.len < 8) return null;
        return std.mem.readInt(u64, val[0..8], .little);
    }
};

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = true };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }
    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn makeRecordKey(txn_id: TxnId) [records_prefix.len + 16]u8 {
    var key_buf: [records_prefix.len + 16]u8 = undefined;
    @memcpy(key_buf[0..records_prefix.len], records_prefix);
    @memcpy(key_buf[records_prefix.len..], &txn_id);
    return key_buf;
}

fn makeSidecarKey(comptime prefix: []const u8, txn_id: TxnId) [prefix.len + 16]u8 {
    var key_buf: [prefix.len + 16]u8 = undefined;
    @memcpy(key_buf[0..prefix.len], prefix);
    @memcpy(key_buf[prefix.len..], &txn_id);
    return key_buf;
}

fn readHasPrefix(read: *backend_erased.ReadTxn, prefix: []const u8) !bool {
    var cursor = try read.openCursor();
    defer cursor.close();
    const entry = (try cursor.seekAtOrAfter(prefix)) orelse return false;
    return std.mem.startsWith(u8, entry.key, prefix);
}

fn participantListCountInRead(
    read: *backend_erased.ReadTxn,
    key: []const u8,
) !usize {
    const raw = read.get(key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    if (raw.len < 4) return TxnError.InvalidTxnRecord;
    var count_buf: [4]u8 = undefined;
    @memcpy(&count_buf, raw[0..4]);
    const count: usize = std.mem.readInt(u32, &count_buf, .little);
    var offset: usize = 4;
    for (0..count) |_| {
        if (raw.len - offset < 4) return TxnError.InvalidTxnRecord;
        var len_buf: [4]u8 = undefined;
        @memcpy(&len_buf, raw[offset .. offset + 4]);
        const len: usize = std.mem.readInt(u32, &len_buf, .little);
        offset += 4;
        if (len > raw.len - offset) return TxnError.InvalidTxnRecord;
        offset += len;
    }
    if (offset != raw.len) return TxnError.InvalidTxnRecord;
    return count;
}

pub fn makeTransactionHABatchOutboxKey(txn_id: TxnId) [ha_batch_outbox_prefix.len + 16]u8 {
    return makeSidecarKey(ha_batch_outbox_prefix, txn_id);
}

pub fn makeTransactionHAReplayOutboxKey(txn_id: TxnId) [ha_replay_outbox_prefix.len + 16]u8 {
    return makeSidecarKey(ha_replay_outbox_prefix, txn_id);
}

fn applyResolveDecision(record: *TxnRecord, status: TxnStatus, timestamp: u64) TxnError!void {
    switch (record.status) {
        .pending => switch (status) {
            .pending => return TxnError.DecisionConflict,
            .committed => {
                record.status = .committed;
                record.commit_version = timestamp;
                record.finalized_at = timestamp;
            },
            .aborted => {
                record.status = .aborted;
                record.finalized_at = timestamp;
            },
        },
        .committed => switch (status) {
            .pending, .aborted => return TxnError.DecisionConflict,
            .committed => {
                if (record.commit_version == 0) record.commit_version = timestamp;
                if (record.finalized_at == 0) record.finalized_at = record.commit_version;
            },
        },
        .aborted => switch (status) {
            .pending, .committed => return TxnError.DecisionConflict,
            .aborted => {
                if (record.finalized_at == 0) record.finalized_at = timestamp;
            },
        },
    }
}

fn resolveDecisionConflictReason(current: TxnStatus, requested: TxnStatus) []const u8 {
    return switch (current) {
        .pending => switch (requested) {
            .pending => "pending->pending",
            .committed => unreachable,
            .aborted => unreachable,
        },
        .committed => switch (requested) {
            .pending => "committed->pending",
            .committed => unreachable,
            .aborted => "committed->aborted",
        },
        .aborted => switch (requested) {
            .pending => "aborted->pending",
            .committed => "aborted->committed",
            .aborted => unreachable,
        },
    };
}

fn decodeRecord(raw: []const u8) !TxnRecord {
    if (raw.len == txn_record_v5_size) {
        if (raw[49] > 1 or raw[50] > 1 or raw[51] > 1) return TxnError.InvalidTxnRecord;
        return .{
            .status = @enumFromInt(raw[0]),
            .begin_timestamp = std.mem.readInt(u64, raw[1..9], .little),
            .commit_version = std.mem.readInt(u64, raw[9..17], .little),
            .created_at = std.mem.readInt(u64, raw[17..25], .little),
            .finalized_at = std.mem.readInt(u64, raw[25..33], .little),
            .intent_revision = std.mem.readInt(u64, raw[33..41], .little),
            .replay_sequence = std.mem.readInt(u64, raw[41..49], .little),
            .prepared = raw[49] == 1,
            .coordinator = raw[50] == 1,
            .retain_terminal = raw[51] == 1,
        };
    }
    if (raw.len == txn_record_v4_size) {
        if (raw[49] > 1 or raw[50] > 1) return TxnError.InvalidTxnRecord;
        return .{
            .status = @enumFromInt(raw[0]),
            .begin_timestamp = std.mem.readInt(u64, raw[1..9], .little),
            .commit_version = std.mem.readInt(u64, raw[9..17], .little),
            .created_at = std.mem.readInt(u64, raw[17..25], .little),
            .finalized_at = std.mem.readInt(u64, raw[25..33], .little),
            .intent_revision = std.mem.readInt(u64, raw[33..41], .little),
            .replay_sequence = std.mem.readInt(u64, raw[41..49], .little),
            .prepared = raw[49] == 1,
            .coordinator = raw[50] == 1,
            .retain_terminal_known = false,
        };
    }
    if (raw.len == txn_record_v3_size) {
        if (raw[49] > 1) return TxnError.InvalidTxnRecord;
        return .{
            .status = @enumFromInt(raw[0]),
            .begin_timestamp = std.mem.readInt(u64, raw[1..9], .little),
            .commit_version = std.mem.readInt(u64, raw[9..17], .little),
            .created_at = std.mem.readInt(u64, raw[17..25], .little),
            .finalized_at = std.mem.readInt(u64, raw[25..33], .little),
            .intent_revision = std.mem.readInt(u64, raw[33..41], .little),
            .replay_sequence = std.mem.readInt(u64, raw[41..49], .little),
            .prepared = raw[49] == 1,
            .coordinator_known = false,
            .retain_terminal_known = false,
        };
    }
    if (raw.len == txn_record_v2_size) {
        return .{
            .status = @enumFromInt(raw[0]),
            .begin_timestamp = std.mem.readInt(u64, raw[1..9], .little),
            .commit_version = std.mem.readInt(u64, raw[9..17], .little),
            .created_at = std.mem.readInt(u64, raw[17..25], .little),
            .finalized_at = std.mem.readInt(u64, raw[25..33], .little),
            .intent_revision = std.mem.readInt(u64, raw[33..41], .little),
            .replay_sequence = std.mem.readInt(u64, raw[41..49], .little),
            .prepared_known = false,
            .coordinator_known = false,
            .retain_terminal_known = false,
        };
    }
    if (raw.len == txn_record_v1_size) {
        return .{
            .status = @enumFromInt(raw[0]),
            .begin_timestamp = std.mem.readInt(u64, raw[1..9], .little),
            .commit_version = std.mem.readInt(u64, raw[9..17], .little),
            .created_at = std.mem.readInt(u64, raw[17..25], .little),
            .finalized_at = std.mem.readInt(u64, raw[25..33], .little),
            .prepared_known = false,
            .coordinator_known = false,
            .retain_terminal_known = false,
        };
    }
    if (raw.len == txn_record_v0_size) {
        const status: TxnStatus = @enumFromInt(raw[0]);
        const ts = std.mem.readInt(u64, raw[1..9], .little);
        return .{
            .status = status,
            .begin_timestamp = ts,
            .commit_version = if (status == .committed) ts else 0,
            .created_at = std.mem.readInt(u64, raw[9..17], .little),
            .finalized_at = 0,
            .prepared_known = false,
            .coordinator_known = false,
            .retain_terminal_known = false,
        };
    }
    return TxnError.InvalidTxnRecord;
}

fn participantListsEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn encodeParticipantList(alloc: Allocator, participants: []const []const u8) ![]u8 {
    var total: usize = 4;
    for (participants) |participant| total += 4 + participant.len;
    const buf = try alloc.alloc(u8, total);
    var count_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &count_buf, @intCast(participants.len), .little);
    @memcpy(buf[0..4], &count_buf);
    var offset: usize = 4;
    for (participants) |participant| {
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(participant.len), .little);
        @memcpy(buf[offset .. offset + 4], &len_buf);
        offset += 4;
        @memcpy(buf[offset .. offset + participant.len], participant);
        offset += participant.len;
    }
    return buf;
}

fn decodeParticipantList(alloc: Allocator, raw: []const u8) ![][]u8 {
    if (raw.len < 4) return TxnError.InvalidTxnRecord;
    var count_buf: [4]u8 = undefined;
    @memcpy(&count_buf, raw[0..4]);
    const count = std.mem.readInt(u32, &count_buf, .little);
    var result = try alloc.alloc([]u8, count);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |entry| alloc.free(entry);
        alloc.free(result);
    }

    var offset: usize = 4;
    var i: usize = 0;
    while (i < count) : (i += 1) {
        if (offset + 4 > raw.len) return TxnError.InvalidTxnRecord;
        var len_buf: [4]u8 = undefined;
        @memcpy(&len_buf, raw[offset .. offset + 4]);
        const len = std.mem.readInt(u32, &len_buf, .little);
        offset += 4;
        if (offset + len > raw.len) return TxnError.InvalidTxnRecord;
        result[i] = try alloc.dupe(u8, raw[offset .. offset + len]);
        initialized += 1;
        offset += len;
    }
    return result;
}

pub fn freeParticipantList(alloc: Allocator, items: [][]u8) void {
    for (items) |entry| alloc.free(entry);
    alloc.free(items);
}

fn putVisibleDoc(store: *DocStore, alloc: Allocator, key: []const u8, value: []const u8) !void {
    const store_key = try internal_keys.documentKeyAlloc(alloc, key);
    defer alloc.free(store_key);
    try store.put(store_key, value);
}

fn getVisibleDoc(store: *DocStore, alloc: Allocator, key: []const u8) ![]u8 {
    const store_key = try internal_keys.documentKeyAlloc(alloc, key);
    defer alloc.free(store_key);
    return try store.get(alloc, store_key);
}

fn getVisibleDocRuntime(store: *backend_erased.Store, alloc: Allocator, key: []const u8) ![]u8 {
    const store_key = try internal_keys.documentKeyAlloc(alloc, key);
    defer alloc.free(store_key);
    var txn = try store.beginRead();
    defer txn.abort();
    return try alloc.dupe(u8, try txn.get(store_key));
}

fn readTimestampRuntime(store: *backend_erased.Store, alloc: Allocator, key: []const u8) !?u64 {
    const ts_key = try internal_keys.ttlKeyAlloc(alloc, key);
    defer alloc.free(ts_key);
    var txn = try store.beginRead();
    defer txn.abort();
    const value = txn.get(ts_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (value.len < 8) return null;
    return std.mem.readInt(u64, value[0..8], .little);
}

fn cleanupTestDir(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

var temp_test_path_nonce: u64 = 0;

fn tempTestPath(alloc: Allocator, label: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &temp_test_path_nonce, .Add, 1, .monotonic);
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-{s}-{d}-{d}", .{
        label,
        platform_time.monotonicNs(),
        nonce,
    });
    defer alloc.free(path);
    return try alloc.dupeZ(u8, path);
}

fn containsKey(keys: []const []const u8, needle: []const u8) bool {
    for (keys) |key| {
        if (std.mem.eql(u8, key, needle)) return true;
    }
    return false;
}

// ============================================================================
// Tests
// ============================================================================

test "transaction init + commit" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-commit");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    const ts: u64 = 1000;

    try mgr.initTransaction(txn_id, ts);
    try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(txn_id));

    // Write intent
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc1", .value = "hello world" },
    }, &.{});

    // Value should NOT be visible at real key yet
    _ = getVisibleDoc(&store, alloc, "doc1") catch |err| {
        try std.testing.expect(err == lmdb.Error.NotFound);
    };

    // Commit
    try mgr.resolveIntents(txn_id, .committed, ts + 1);
    try std.testing.expectEqual(TxnStatus.committed, try mgr.getTransactionStatus(txn_id));
    try std.testing.expectEqual(ts + 1, try mgr.getCommitVersion(txn_id));

    // Now value should be visible
    const val = try getVisibleDoc(&store, alloc, "doc1");
    defer alloc.free(val);
    try std.testing.expectEqualStrings("hello world", val);

    // Timestamp should be written
    const doc_ts = try ttl.readTimestamp(&store, alloc, "doc1");
    try std.testing.expect(doc_ts != null);
    try std.testing.expectEqual(ts + 1, doc_ts.?);
}

test "transaction manager works with memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var mgr = try TxnManager.init(alloc, &runtime);
    defer mgr.deinit();

    const txn_id: TxnId = .{ 4, 3, 2, 1, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2 };
    const ts: u64 = 42_000;

    try mgr.initTransaction(txn_id, ts);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc_mem", .value = "hello mem" },
    }, &.{});

    _ = getVisibleDocRuntime(&mgr.store, alloc, "doc_mem") catch |err| {
        try std.testing.expect(err == error.NotFound);
    };

    try mgr.resolveIntents(txn_id, .committed, ts + 1);

    const value = try getVisibleDocRuntime(&mgr.store, alloc, "doc_mem");
    defer alloc.free(value);
    try std.testing.expectEqualStrings("hello mem", value);
    try std.testing.expectEqual(@as(?u64, ts + 1), try readTimestampRuntime(&mgr.store, alloc, "doc_mem"));
}

test "transaction manager works with lsm backend store" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var mgr = try TxnManager.init(alloc, &runtime);
    defer mgr.deinit();

    const txn_id: TxnId = .{ 6, 5, 4, 3, 2, 1, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
    const ts: u64 = 52_000;

    try mgr.initTransaction(txn_id, ts);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc_lsm", .value = "hello lsm" },
    }, &.{});

    _ = getVisibleDocRuntime(&mgr.store, alloc, "doc_lsm") catch |err| {
        try std.testing.expect(err == error.NotFound);
    };

    try mgr.resolveIntents(txn_id, .committed, ts + 1);

    const value = try getVisibleDocRuntime(&mgr.store, alloc, "doc_lsm");
    defer alloc.free(value);
    try std.testing.expectEqualStrings("hello lsm", value);
    try std.testing.expectEqual(@as(?u64, ts + 1), try readTimestampRuntime(&mgr.store, alloc, "doc_lsm"));
}

test "transaction record preserves begin timestamp separately from commit version" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-record-versions");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3 };
    const begin_ts: u64 = 5_000;
    const commit_ts: u64 = 6_000;

    try mgr.initTransaction(txn_id, begin_ts);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc_versioned", .value = "value" },
    }, &.{});
    try mgr.resolveIntents(txn_id, .committed, commit_ts);

    const raw = try store.get(alloc, &makeRecordKey(txn_id));
    defer alloc.free(raw);
    const record = try decodeRecord(raw);
    try std.testing.expectEqual(begin_ts, record.begin_timestamp);
    try std.testing.expectEqual(commit_ts, record.commit_version);
    try std.testing.expectEqual(commit_ts, record.visibleVersion());
    try std.testing.expectEqual(commit_ts, record.finalized_at);
}

test "transaction protocol fences begin prepare snapshot and idempotent resolution" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-protocol-fencing");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6 };

    try mgr.initTransactionWithParticipants(txn_id, 10_000, &.{ "docs:1", "docs:2" });
    try mgr.initTransactionWithParticipants(txn_id, 10_000, &.{ "docs:1", "docs:2" });
    try std.testing.expectError(TxnError.DecisionConflict, mgr.initTransactionWithParticipants(txn_id, 10_000, &.{"docs:1"}));

    try mgr.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "a" }}, &.{});
    var first_snapshot = try mgr.collectIntentBatch(alloc, txn_id);
    defer first_snapshot.deinit(alloc);
    try mgr.writeIntents(txn_id, &.{.{ .key = "doc:b", .value = "b" }}, &.{});
    try std.testing.expectError(error.IntentSnapshotChanged, mgr.validateIntentSnapshot(txn_id, first_snapshot.revision));
    try std.testing.expectError(error.IntentSnapshotChanged, mgr.resolveIntentsWithExtraBatch(
        txn_id,
        .committed,
        11_000,
        .{ .expected_intent_revision = first_snapshot.revision },
    ));

    var final_snapshot = try mgr.collectIntentBatch(alloc, txn_id);
    defer final_snapshot.deinit(alloc);
    const committed = try mgr.resolveIntentsWithExtraBatch(txn_id, .committed, 11_000, .{
        .expected_intent_revision = final_snapshot.revision,
        .replay = .{ .sequence = 7, .payload = "opaque" },
    });
    try std.testing.expect(committed.applied);
    try std.testing.expectEqual(@as(u64, 7), committed.replay_sequence);

    try std.testing.expectError(TxnError.DecisionConflict, mgr.writeIntents(txn_id, &.{.{ .key = "doc:c", .value = "c" }}, &.{}));
    try std.testing.expectError(TxnError.DecisionConflict, mgr.initTransactionWithParticipants(txn_id, 10_000, &.{ "docs:1", "docs:2" }));
    const repeated = try mgr.resolveIntentsWithExtraBatch(txn_id, .committed, 11_000, .{
        .expected_intent_revision = final_snapshot.revision,
    });
    try std.testing.expect(!repeated.applied);
    try std.testing.expectEqual(@as(u64, 7), repeated.replay_sequence);
}

test "idempotent begin upgrades a legacy transaction coordinator role" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-upgrade-legacy-coordinator-role");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{6} ** 16;
    const participants = [_][]const u8{ "table2:4:docs:group:7", "table2:4:docs:group:8" };
    try mgr.initTransactionWithParticipantsCreatedAtAndRole(txn_id, 1_000, 900, &participants, false);

    var legacy: [txn_record_v3_size]u8 = @splat(0);
    legacy[0] = @intFromEnum(TxnStatus.pending);
    std.mem.writeInt(u64, legacy[1..9], 1_000, .little);
    std.mem.writeInt(u64, legacy[17..25], 900, .little);
    const record_key = makeRecordKey(txn_id);
    try mgr.putValue(&record_key, &legacy);

    try mgr.initTransactionWithParticipantsCreatedAtAndRole(txn_id, 1_000, 900, &participants, true);
    const txns = try mgr.listTransactions(alloc);
    defer alloc.free(txns);
    try std.testing.expectEqual(@as(usize, 1), txns.len);
    try std.testing.expect(txns[0].coordinator_known);
    try std.testing.expect(txns[0].coordinator);

    const txn_id_2: TxnId = .{7} ** 16;
    const txn_id_3: TxnId = .{8} ** 16;
    try mgr.initTransaction(txn_id_2, 1_001);
    try mgr.initTransaction(txn_id_3, 1_002);
    const first_page = try mgr.listTransactionsPage(alloc, null, 2);
    defer alloc.free(first_page.items);
    try std.testing.expectEqual(@as(usize, 2), first_page.items.len);
    try std.testing.expect(first_page.next_after != null);
    const second_page = try mgr.listTransactionsPage(alloc, first_page.next_after, 2);
    defer alloc.free(second_page.items);
    try std.testing.expectEqual(@as(usize, 1), second_page.items.len);
    try std.testing.expect(second_page.next_after == null);
}

test "transaction init + abort" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-abort");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    const ts: u64 = 2000;

    try mgr.initTransaction(txn_id, ts);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc_abort", .value = "should not appear" },
    }, &.{});

    // Abort
    try mgr.resolveIntents(txn_id, .aborted, ts + 1);
    try std.testing.expectEqual(TxnStatus.aborted, try mgr.getTransactionStatus(txn_id));

    // Value should NOT be visible
    _ = getVisibleDoc(&store, alloc, "doc_abort") catch |err| {
        try std.testing.expect(err == lmdb.Error.NotFound);
        return;
    };
    // If we got here, the key exists when it shouldn't
    return error.TestUnexpectedResult;
}

test "version predicate conflict" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-vp");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2 };

    // Write a key with a known timestamp
    try putVisibleDoc(&store, alloc, "existing_key", "value");
    try ttl.writeTimestamp(&store, "existing_key", 5000);

    try mgr.initTransaction(txn_id, 6000);

    // Predicate: key must not exist (expected_version=0) — should conflict
    const result1 = mgr.writeIntents(txn_id, &.{
        .{ .key = "existing_key", .value = "new_value" },
    }, &.{
        .{ .key = "existing_key", .expected_version = 0 },
    });
    try std.testing.expectError(TxnError.VersionConflict, result1);

    // Predicate: wrong version — should conflict
    const result2 = mgr.writeIntents(txn_id, &.{
        .{ .key = "existing_key", .value = "new_value" },
    }, &.{
        .{ .key = "existing_key", .expected_version = 9999 },
    });
    try std.testing.expectError(TxnError.VersionConflict, result2);

    // Predicate: correct version — should succeed
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "existing_key", .value = "new_value" },
    }, &.{
        .{ .key = "existing_key", .expected_version = 5000 },
    });
}

test "concurrent intent conflict" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-conflict");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn1: TxnId = .{ 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const txn2: TxnId = .{ 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    try mgr.initTransaction(txn1, 1000);
    try mgr.initTransaction(txn2, 1001);

    // Txn1 writes intent on "shared_key"
    try mgr.writeIntents(txn1, &.{
        .{ .key = "shared_key", .value = "from_txn1" },
    }, &.{});

    // Txn2 tries to write intent on same key — should conflict
    const result = mgr.writeIntents(txn2, &.{
        .{ .key = "shared_key", .value = "from_txn2" },
    }, &.{});
    try std.testing.expectError(TxnError.IntentConflict, result);
}

test "transaction delete intent" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-delete");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();

    // First, put a value directly
    try putVisibleDoc(&store, alloc, "to_delete", "original");

    const txn_id: TxnId = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3 };
    try mgr.initTransaction(txn_id, 3000);

    // Write a delete intent (value = null)
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "to_delete", .value = null },
    }, &.{});

    // Key should still exist before commit
    const before = try getVisibleDoc(&store, alloc, "to_delete");
    defer alloc.free(before);
    try std.testing.expectEqualStrings("original", before);

    // Commit
    try mgr.resolveIntents(txn_id, .committed, 3001);

    // Key should be deleted
    _ = getVisibleDoc(&store, alloc, "to_delete") catch |err| {
        try std.testing.expect(err == lmdb.Error.NotFound);
        return;
    };
    return error.TestUnexpectedResult;
}

test "getTransactionStatus not found" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-notfound");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const missing: TxnId = .{ 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99 };

    const result = mgr.getTransactionStatus(missing);
    try std.testing.expectError(TxnError.TxnNotFound, result);
}

test "getCommitVersion not found" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-commit-version-notfound");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const missing: TxnId = .{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };

    const result = mgr.getCommitVersion(missing);
    try std.testing.expectError(TxnError.TxnNotFound, result);
}

test "recoverTransactions auto-aborts stale pending transactions" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-stale-pending");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 };
    try mgr.initTransaction(txn_id, 1_000);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:stale_pending", .value = "pending" },
    }, &.{});

    const stats = try mgr.recoverTransactions(2_000, 3_000);
    try std.testing.expectEqual(@as(u64, 1), stats.scanned_records);
    try std.testing.expectEqual(@as(u64, 1), stats.auto_aborted);
    try std.testing.expectEqual(TxnStatus.aborted, try mgr.getTransactionStatus(txn_id));

    _ = getVisibleDoc(&store, alloc, "doc:stale_pending") catch |err| {
        try std.testing.expect(err == lmdb.Error.NotFound);
    };

    const intent_key = try mgr.makeIntentKey(txn_id, "doc:stale_pending");
    defer alloc.free(intent_key);
    _ = store.get(alloc, intent_key) catch |err| {
        try std.testing.expect(err == lmdb.Error.NotFound);
        return;
    };
    return error.TestUnexpectedResult;
}

test "recoverTransactions never presumes abort after a distributed prepare vote" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-stale-prepared-participant");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4 };
    try mgr.initTransactionWithParticipantsCreatedAt(txn_id, 10_000, 1_000, &.{ "group:a", "group:b" });
    try mgr.writeIntents(txn_id, &.{.{ .key = "doc:prepared", .value = "prepared" }}, &.{});

    const stats = try mgr.recoverTransactions(2_000, 3_000);
    try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
    try std.testing.expectEqual(@as(u64, 1), stats.kept_recent_pending);
    try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(txn_id));

    const intent_key = try mgr.makeIntentKey(txn_id, "doc:prepared");
    defer alloc.free(intent_key);
    const intent = try store.get(alloc, intent_key);
    defer alloc.free(intent);
    try std.testing.expectEqualStrings("prepared", intent[1..]);
}

test "coordinator recovery durably aborts a stale prepared transaction" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-stale-prepared-coordinator");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{9} ** 16;
    try mgr.initTransactionWithParticipantsCreatedAtAndRole(
        txn_id,
        10_000,
        1_000,
        &.{ "group:a", "group:b" },
        true,
    );
    try mgr.writeIntents(txn_id, &.{.{ .key = "doc:prepared", .value = "prepared" }}, &.{});

    const stats = try mgr.recoverTransactions(2_000, 3_000);
    try std.testing.expectEqual(@as(u64, 1), stats.auto_aborted);
    try std.testing.expectEqual(TxnStatus.aborted, try mgr.getTransactionStatus(txn_id));
}

test "transaction recovery age is independent from logical begin timestamp" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-created-at-independent");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 7, 4, 7, 4, 7, 4, 7, 4, 7, 4, 7, 4, 7, 4, 7, 4 };
    try mgr.initTransactionWithParticipantsCreatedAt(txn_id, 1_000, 10_000, &.{"group:a"});

    const stats = try mgr.recoverTransactions(5_000, 12_000);
    try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
    try std.testing.expectEqual(@as(u64, 1), stats.kept_recent_pending);
    try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(txn_id));
}

test "recoverTransactions keeps recent pending transactions" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-recent-pending");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 };
    try mgr.initTransaction(txn_id, 2_500);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:recent_pending", .value = "pending" },
    }, &.{});

    const stats = try mgr.recoverTransactions(2_000, 3_000);
    try std.testing.expectEqual(@as(u64, 1), stats.scanned_records);
    try std.testing.expectEqual(@as(u64, 1), stats.kept_recent_pending);
    try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(txn_id));

    const intent_key = try mgr.makeIntentKey(txn_id, "doc:recent_pending");
    defer alloc.free(intent_key);
    const intent_val = try store.get(alloc, intent_key);
    defer alloc.free(intent_val);
    try std.testing.expect(intent_val.len > 0);
}

test "recoverTransactions resolves committed orphaned intents and cleans old record" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-committed-orphan");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6 };
    try mgr.initTransaction(txn_id, 1_000);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:orphan_commit", .value = "committed" },
    }, &.{});

    const committed = TxnRecord{
        .status = .committed,
        .begin_timestamp = 1_000,
        .commit_version = 2_000,
        .created_at = 1_000,
        .finalized_at = 2_000,
    };
    try mgr.saveTransactionRecord(makeRecordKey(txn_id), committed);

    const stats = try mgr.recoverTransactions(3_000, 4_000);
    try std.testing.expectEqual(@as(u64, 1), stats.resolved_finalized);
    try std.testing.expectEqual(@as(u64, 1), stats.cleaned_records);

    const doc = try getVisibleDoc(&store, alloc, "doc:orphan_commit");
    defer alloc.free(doc);
    try std.testing.expectEqualStrings("committed", doc);
    try std.testing.expectEqual(@as(?u64, 2_000), try ttl.readTimestamp(&store, alloc, "doc:orphan_commit"));
    try std.testing.expectError(TxnError.TxnNotFound, mgr.getTransactionStatus(txn_id));
}

test "recoverTransactions appends extra resolution batch for committed orphaned intents" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-committed-extra");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16 };
    try mgr.initTransaction(txn_id, 1_000);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:orphan_extra", .value = "committed" },
    }, &.{});

    const committed = TxnRecord{
        .status = .committed,
        .begin_timestamp = 1_000,
        .commit_version = 2_000,
        .created_at = 1_000,
        .finalized_at = 2_000,
    };
    try mgr.saveTransactionRecord(makeRecordKey(txn_id), committed);

    const Hook = struct {
        const extra_key = "\x00\x00__metadata__:txn-extra";
        const extra_value = "seen";

        fn build(
            ctx: ?*anyopaque,
            manager: *TxnManager,
            hook_txn_id: TxnId,
            status: TxnStatus,
            timestamp: u64,
        ) anyerror!ResolutionExtraBatch {
            _ = manager;
            _ = hook_txn_id;
            _ = timestamp;
            try std.testing.expectEqual(TxnStatus.committed, status);
            const hook_alloc: *Allocator = @ptrCast(@alignCast(ctx.?));
            const writes = try hook_alloc.alloc(docstore.KVPair, 1);
            errdefer hook_alloc.free(writes);
            writes[0] = .{
                .key = try hook_alloc.dupe(u8, extra_key),
                .value = try hook_alloc.dupe(u8, extra_value),
            };
            return .{ .writes = writes };
        }

        fn cleanup(ctx: ?*anyopaque, batch: ResolutionExtraBatch) void {
            const hook_alloc: *Allocator = @ptrCast(@alignCast(ctx.?));
            for (batch.writes) |item| {
                hook_alloc.free(@constCast(item.key));
                hook_alloc.free(@constCast(item.value));
            }
            if (batch.writes.len > 0) hook_alloc.free(@constCast(batch.writes));
        }
    };

    var hook_alloc = alloc;
    const stats = try mgr.recoverTransactionsWithExtraBatchHooks(3_000, 4_000, .{
        .ctx = &hook_alloc,
        .build = Hook.build,
        .cleanup = Hook.cleanup,
    });
    try std.testing.expectEqual(@as(u64, 1), stats.resolved_finalized);
    try std.testing.expectEqual(@as(u64, 1), stats.cleaned_records);

    const extra = try store.get(alloc, Hook.extra_key);
    defer alloc.free(extra);
    try std.testing.expectEqualStrings(Hook.extra_value, extra);
}

test "recoverTransactions cleans aborted orphaned intents and old record" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-aborted-orphan");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };
    try mgr.initTransaction(txn_id, 1_500);
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:orphan_abort", .value = "aborted" },
    }, &.{});

    const aborted = TxnRecord{
        .status = .aborted,
        .begin_timestamp = 1_500,
        .commit_version = 0,
        .created_at = 1_500,
        .finalized_at = 2_500,
    };
    try mgr.saveTransactionRecord(makeRecordKey(txn_id), aborted);

    const stats = try mgr.recoverTransactions(3_000, 4_000);
    try std.testing.expectEqual(@as(u64, 1), stats.resolved_finalized);
    try std.testing.expectEqual(@as(u64, 1), stats.cleaned_records);

    _ = getVisibleDoc(&store, alloc, "doc:orphan_abort") catch |err| {
        try std.testing.expect(err == lmdb.Error.NotFound);
    };
    try std.testing.expectError(TxnError.TxnNotFound, mgr.getTransactionStatus(txn_id));
}

test "transaction participants track unresolved members" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-participants");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 };
    try mgr.initTransactionWithParticipants(txn_id, 1_000, &.{ "shard-a", "shard-b" });

    const participants = try mgr.getParticipants(alloc, txn_id);
    defer freeParticipantList(alloc, participants);
    try std.testing.expectEqual(@as(usize, 2), participants.len);

    const unresolved_initial = try mgr.getUnresolvedParticipants(alloc, txn_id);
    defer freeParticipantList(alloc, unresolved_initial);
    try std.testing.expectEqual(@as(usize, 2), unresolved_initial.len);

    try mgr.markParticipantResolved(txn_id, "shard-a");

    const unresolved_after = try mgr.getUnresolvedParticipants(alloc, txn_id);
    defer freeParticipantList(alloc, unresolved_after);
    try std.testing.expectEqual(@as(usize, 1), unresolved_after.len);
    try std.testing.expectEqualStrings("shard-b", unresolved_after[0]);
}

test "topology fence retains committed coordinator recovery obligations" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-topology-recovery-fence");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 4, 2, 4, 2, 4, 2, 4, 2, 4, 2, 4, 2, 4, 2, 4, 2 };
    try mgr.initTransactionWithParticipantsCreatedAtRoleAndRetention(
        txn_id,
        1_000,
        900,
        &.{ "local", "remote" },
        true,
        true,
    );
    try mgr.resolveIntents(txn_id, .committed, 2_000);
    try std.testing.expect(try mgr.defersCoordinatorAcknowledgement(txn_id));

    // A retained stable coordinator keeps its own acknowledgement unresolved
    // until the API session result is durable. Even after remote propagation
    // completes, topology must remain fenced across that handoff window.
    try std.testing.expect(try mgr.hasTopologySensitiveTransactions());
    try mgr.markParticipantResolved(txn_id, "remote");
    try std.testing.expect(try mgr.hasTopologySensitiveTransactions());
    try mgr.markParticipantResolved(txn_id, "local");
    try std.testing.expect(!try mgr.hasTopologySensitiveTransactions());

    // HA delivery debt is equally topology-sensitive even after every 2PC
    // participant has acknowledged the decision.
    const outbox_key = makeTransactionHABatchOutboxKey(txn_id);
    try mgr.putValue(&outbox_key, "pending-mirror-delivery");
    try std.testing.expect(try mgr.hasTopologySensitiveTransactions());
    try mgr.clearHAOutbox(txn_id, .batch);
    try std.testing.expect(!try mgr.hasTopologySensitiveTransactions());
}

test "recoverTransactions preserves finalized record while participants remain unresolved" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-unresolved-participants");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };
    try mgr.initTransactionWithParticipants(txn_id, 1_000, &.{ "local", "remote" });
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:participant_defer", .value = "value" },
    }, &.{});
    try mgr.resolveIntents(txn_id, .committed, 2_000);
    try mgr.markParticipantResolved(txn_id, "local");

    const stats = try mgr.recoverTransactions(3_000, 4_000);
    try std.testing.expectEqual(@as(u64, 1), stats.deferred_unresolved);
    try std.testing.expectEqual(@as(u64, 0), stats.cleaned_records);
    try std.testing.expectEqual(TxnStatus.committed, try mgr.getTransactionStatus(txn_id));
}

test "late committed resolve after stale auto-abort does not silently lose write" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-late-commit-after-auto-abort");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 2, 4, 2, 4, 2, 4, 2, 4, 2, 4, 2, 4, 2, 4, 2, 4 };
    // Auto-abort is a coordinator decision. A prepared non-coordinator must
    // retain its intents until it learns the replicated decision instead.
    try mgr.initTransactionWithParticipantsCreatedAtAndRole(
        txn_id,
        1_000,
        1_000,
        &.{ "coordinator", "participant" },
        true,
    );
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:late_commit_after_abort", .value = "committed-value" },
    }, &.{});

    const recovered = try mgr.recoverTransactions(2_000, 3_000);
    try std.testing.expectEqual(@as(u64, 1), recovered.auto_aborted);
    try std.testing.expectEqual(TxnStatus.aborted, try mgr.getTransactionStatus(txn_id));

    try std.testing.expectError(TxnError.DecisionConflict, mgr.resolveIntents(txn_id, .committed, 4_000));
    try std.testing.expectEqual(TxnStatus.aborted, try mgr.getTransactionStatus(txn_id));

    const doc = getVisibleDoc(&store, alloc, "doc:late_commit_after_abort") catch |err| switch (err) {
        lmdb.Error.NotFound => null,
        else => return err,
    };
    defer if (doc) |value| alloc.free(value);

    const ts = try ttl.readTimestamp(&store, alloc, "doc:late_commit_after_abort");

    try std.testing.expect(doc == null);
    try std.testing.expect(ts == null);
}

test "recoverTransactions cleans finalized record after all participants resolve" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-recover-all-participants-resolved");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = .{ 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 };
    try mgr.initTransactionWithParticipants(txn_id, 1_000, &.{ "local", "remote" });
    try mgr.writeIntents(txn_id, &.{
        .{ .key = "doc:participant_clean", .value = "value" },
    }, &.{});
    try mgr.resolveIntents(txn_id, .committed, 2_000);
    try mgr.markParticipantResolved(txn_id, "local");
    try mgr.markParticipantResolved(txn_id, "remote");

    const stats = try mgr.recoverTransactions(3_000, 4_000);
    try std.testing.expectEqual(@as(u64, 1), stats.cleaned_records);
    try std.testing.expectError(TxnError.TxnNotFound, mgr.getTransactionStatus(txn_id));
}

test "retained terminal transactions honor the extended retry cutoff" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "retained-terminal" });
    defer runtime_store.deinit();

    var mgr = try TxnManager.init(alloc, &runtime_store);
    defer mgr.deinit();
    const txn_id: TxnId = .{5} ** 16;
    try mgr.initTransactionWithParticipantsCreatedAtRoleAndRetention(
        txn_id,
        1_000,
        1_000,
        &.{},
        true,
        true,
    );
    try mgr.resolveIntents(txn_id, .committed, 2_000);

    const retained = try mgr.recoverTransactionsWithExtraBatchHooksAndOptions(
        3_000,
        4_000,
        .{},
        .{ .retained_cutoff_timestamp = 1_500 },
    );
    try std.testing.expectEqual(@as(u64, 0), retained.cleaned_records);
    try std.testing.expectEqual(TxnStatus.committed, try mgr.getTransactionStatus(txn_id));

    const expired = try mgr.recoverTransactionsWithExtraBatchHooksAndOptions(
        3_000,
        4_000,
        .{},
        .{ .retained_cutoff_timestamp = 3_000 },
    );
    try std.testing.expectEqual(@as(u64, 1), expired.cleaned_records);
    try std.testing.expectError(TxnError.TxnNotFound, mgr.getTransactionStatus(txn_id));
    try std.testing.expectError(TxnError.TxnNotFound, mgr.markParticipantResolved(txn_id, "late"));
    const resolved = try mgr.getResolvedParticipants(alloc, txn_id);
    defer freeParticipantList(alloc, resolved);
    try std.testing.expectEqual(@as(usize, 0), resolved.len);
}
