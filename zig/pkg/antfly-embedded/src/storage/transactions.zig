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
const read_control = @import("../common/read_admission.zig");

/// Remember original rejection across missing-record recovery branches. The
/// adapter stays at its bound address for the synchronous mutation call only.
pub const BeginAdmissionTrace = MutationAdmissionTrace;
pub const MutationAdmissionTrace = read_control.RejectionTrace;
const docstore = @import("docstore.zig");
const DocStore = docstore.DocStore;
const internal_keys = @import("internal_keys.zig");
const range_protection = @import("range_protection.zig");
const lsm_backend = @import("lsm_backend.zig");
const mem_backend = @import("mem_backend.zig");
const platform_time = @import("antfly_platform").time;
const build_options = @import("build_options");
const tracing = @import("../tracing/antfly_trace_writer.zig");
const stderr_writer = @import("../tracing/stderr_writer.zig");
const ttl = @import("ttl.zig");
const retained_effects = @import("retained_effects.zig");

// ============================================================================
// Key prefixes
// ============================================================================

const intents_prefix = "\x00\x00__txn_intents__:";
// Key-oriented companion index for O(1) conflict checks on ordinary writes.
// The value is the owning transaction ID. It is installed and removed in the
// same backend batch as the corresponding intent, so the DB apply mutex makes
// lock acquisition and ordinary batch admission linearizable.
const intent_locks_prefix = "\x00\x00__txn_intent_locks__:";
// Pending transactions retain their exact user keys so collection and
// resolution use point probes instead of cloning the whole mutable memtable.
// Resolution deletes this sidecar atomically with the intents; terminal
// history uses TxnRecord.intents_resolved instead.
const intent_keys_prefix = "\x00\x00__txn_intent_keys__:";
const intent_members_prefix = "\x00\x00__txn_intent_members__:";
const intent_admission_prefix = "\x00\x00__txn_intent_admission__:";
const IntentAdmission = struct {
    count: u64 = 0,
    bytes: u64 = 0,
    retained_bytes: u64 = 0,
    retained_keys: u64 = 0,
    retention_tracked: bool = true,

    fn reservation(self: @This()) u64 {
        if (!self.retention_tracked or self.retained_keys == 0) return 0;
        if (self.retained_keys > retained_effects.max_keys) return @max(self.retained_bytes +| 48, retained_effects.max_frame_bytes + 1);
        return self.retained_bytes +| 48;
    }
};
const IntentCost = struct { bytes: u64, retained: u64, retained_keys: u64 = 0 };
// Read dependencies that are not also writes need durable shared guards.
// A predicate check alone is not a prepare vote: another transaction could
// delete its parent row after the check but before the coordinator commits.
// Readers share key-oriented entries, while the transaction-oriented members
// make resolution proportional to its own read set (never all readers).
const read_guards_prefix = "\x00\x00__txn_read_guards__:";
const read_members_prefix = "\x00\x00__txn_read_members__:";
const read_admission_prefix = "\x00\x00__txn_read_admission__:";
const read_guard_count_key = "\x00\x00__txn_read_guard_count";
pub const max_read_guards_per_transaction = 65_536;
// Durable epoch leases. A prepare vote and its schema identity are one atomic
// mutation; resolution retires both. Historical immutable schemas remain
// usable after a new active epoch is published and after participant restart.
const schema_leases_prefix = "\x00\x00__txn_schema_leases__:";
pub const records_prefix = "\x00\x00__txn_records__:";
const participants_prefix = "\x00\x00__txn_participants__:";
const participant_index_prefix = "\x00\x00__txn_participant_index_v1__:";
const resolved_index_prefix = "\x00\x00__txn_resolved_index_v1__:";
const resolved_participants_prefix = "\x00\x00__txn_resolved_participants__:";
const publication_backlog = @import("publication_backlog.zig");
const prepared_effects = @import("db/prepared_transaction_effects.zig");
const hot_standby_batch_outbox_prefix = publication_backlog.transaction_batch_prefix;
const hot_standby_replay_outbox_prefix = "\x00\x00__txn_ha_replay_outbox__:";

// ============================================================================
// Types
// ============================================================================

pub const TxnId = [16]u8;

pub fn makePreparedEffectsKey(txn_id: TxnId) [publication_backlog.prepared_effects_prefix.len + 16]u8 {
    return publication_backlog.prepared_effects_prefix.* ++ txn_id;
}

pub fn makePreparationRequirementKey(txn_id: TxnId) [publication_backlog.preparation_requirement_prefix.len + 16]u8 {
    return publication_backlog.preparation_requirement_prefix.* ++ txn_id;
}

pub const TxnStatus = enum(u8) {
    pending = 0,
    committed = 1,
    aborted = 2,
};

pub const WriteIntent = struct {
    key: []const u8,
    value: ?[]const u8, // null for deletes
    /// Borrowed logical typing metadata; must be consumed by canonical AROW
    /// preparation before the journal boundary, never silently discarded.
    json_null_fields: []const []const u8 = &.{},
    /// Schema-bound canonical AROW produced before voting prepared. The API
    /// sidecar retains only API-only special fields needed by commit effects.
    prepared_row: ?[]const u8 = null,
    /// Native preparation overwrites these bounds from the pinned vector
    /// catalog; public caller estimates are never used as admission authority.
    retained_artifact_bytes: u64 = 0,
    retained_artifact_keys: u64 = 0,
};

pub const IntentValue = struct {
    value: ?[]const u8,
    prepared_row: ?[]const u8 = null,

    pub fn decode(bytes: []const u8) !IntentValue {
        if (bytes.len == 0) return error.InvalidTxnRecord;
        return switch (bytes[0]) {
            0 => .{ .value = bytes[1..] },
            1 => if (bytes.len == 1) .{ .value = null } else error.InvalidTxnRecord,
            2 => blk: {
                if (bytes.len < 5) return error.InvalidTxnRecord;
                const length = std.mem.readInt(u32, bytes[1..5], .little);
                if (length > bytes.len - 5 or length == bytes.len - 5) return error.InvalidTxnRecord;
                break :blk .{ .value = bytes[5..][0..length], .prepared_row = bytes[5 + length ..] };
            },
            else => error.InvalidTxnRecord,
        };
    }
};

/// Admission counts retained input/AROW bytes and per-row commit metadata.
/// Transient parsing and preparation allocations are independently charged by
/// the preparation allocator, rather than multiplying payload bytes by a DOM
/// worst case. Limits apply across prepares; replacing a key releases credits.
pub fn intentAdmissionBytes(intent: WriteIntent) !u64 {
    return intentAdmissionBytesWithAdmission(intent, null);
}

pub fn intentAdmissionBytesWithAdmission(intent: WriteIntent, control: ?read_control.Port) !u64 {
    if (control) |owner| try owner.checkpoint();
    var payload: u64 = (if (intent.value) |value| value.len else 0) +
        (if (intent.prepared_row) |row| row.len else 0);
    for (intent.json_null_fields, 0..) |field, index| {
        if (index % 64 == 0) if (control) |owner| try owner.checkpoint();
        const field_bytes = std.math.add(u64, field.len, @sizeOf([]const u8)) catch return error.TransactionTooLarge;
        payload = std.math.add(u64, payload, field_bytes) catch return error.TransactionTooLarge;
    }
    if (control) |owner| try owner.checkpoint();
    const bytes = payload;
    const keys = std.math.mul(u64, intent.key.len, 16) catch return error.TransactionTooLarge;
    return std.math.add(u64, bytes, std.math.add(u64, keys, 4096) catch return error.TransactionTooLarge) catch error.TransactionTooLarge;
}

/// REF3 contains final primary effects/timestamps and integrity records, not
/// derived index/vector writes. AROW metadata replacement preserves length.
/// Document numbers retain their exact source spelling. Only special-field
/// stripping serializes JSON; count its exact output with a bounded writer.
fn isRawMetadataIntentKey(key: []const u8) bool {
    return range_protection.counterBucket(key) != null or
        range_protection.indexCounterDigest(key) != null or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:relational_integrity:") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_activation") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_retirement") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_integrity_generation_retirement") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:restore_staging_owner") or
        @import("db/relational_index_maintenance_contract.zig").isControlKey(key);
}

fn isRawMetadataPredicateKey(key: []const u8) bool {
    return isRawMetadataIntentKey(key) or
        std.mem.eql(u8, key, @import("db/relational_index_catalog.zig").head_key) or
        (key.len == @import("db/relational_index_jobs.zig").progress_prefix.len + range_protection.index_id_bytes and
            std.mem.startsWith(u8, key, @import("db/relational_index_jobs.zig").progress_prefix));
}

fn retainedIntentBytes(alloc: Allocator, intent: WriteIntent) !u64 {
    return retainedIntentBytesWithAdmission(alloc, intent, null);
}

pub fn retainedIntentBytesWithAdmission(alloc: Allocator, intent: WriteIntent, control: ?read_control.Port) !u64 {
    if (control) |owner| try owner.checkpoint();
    if (@import("db/relational_integrity_contract.zig").isKey(intent.key)) {
        _ = try @import("db/relational_integrity_contract.zig").parseKey(intent.key);
        return std.math.add(u64, 16 + intent.key.len, if (intent.value) |value| value.len else 0) catch error.TransactionTooLarge;
    }
    if (isRawMetadataIntentKey(intent.key)) return 0;
    var payload: u64 = if (intent.prepared_row) |row| row.len else if (intent.value) |value| value.len else 0;
    if (intent.prepared_row == null) if (intent.value) |value| {
        payload = try @import("transaction_intent_admission.zig").payloadBytes(alloc, value, control);
    };
    if (control) |owner| try owner.checkpoint();
    const primary = std.math.add(u64, 16 + 2 + try @import("transaction_intent_admission.zig").encodedComponentBytes(intent.key, control), payload) catch return error.TransactionTooLarge;
    return std.math.add(u64, primary, intent.retained_artifact_bytes) catch error.TransactionTooLarge;
}

fn retainedIntentKeys(intent: WriteIntent, retained: u64) !u64 {
    if (retained == 0) return 0;
    return std.math.add(u64, 1, intent.retained_artifact_keys) catch error.TransactionTooLarge;
}

fn appendOwnedBytes(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8), bytes: []u8) !void {
    errdefer alloc.free(bytes);
    try list.append(alloc, bytes);
}

pub const OwnedIntentMutation = struct {
    key: []u8,
    value: ?[]u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.key);
        if (self.value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const VersionPredicate = struct {
    key: []const u8,
    expected_version: u64 = 0, // 0 = key must not exist
    expected_content_digest: ?[32]u8 = null,
    /// A server-authored INSERT identity constraint, rather than an observed read.
    unique_absence: bool = false,
    /// Internal integrity records are raw metadata, not timestamped primary
    /// rows. Their CAS compares complete bytes and retains the same shared
    /// dependency guard as a document predicate. NULL means absent, not empty.
    comparison: enum { document_version, exact_value } = .document_version,
    expected_value: ?[]const u8 = null,
    // Successful prepare retains this dependency through terminal resolution.
    // A predicate on a write key uses that write's exclusive intent instead.
};

fn validReadMember(bytes: []const u8) bool {
    return bytes.len == 8 or (bytes.len == 33 and (bytes[0] == 2 or bytes[0] == 3 or bytes[0] == 4));
}

fn readPredicateIdentity(predicate: VersionPredicate, out: *[33]u8) []const u8 {
    return readPredicateIdentityWithAdmission(predicate, out, null) catch unreachable;
}

fn readPredicateIdentityWithAdmission(predicate: VersionPredicate, out: *[33]u8, control: ?read_control.Port) ![]const u8 {
    if (control) |owner| try owner.checkpoint();
    switch (predicate.comparison) {
        .document_version => {
            if (predicate.expected_content_digest) |digest| {
                var version: [8]u8 = undefined;
                std.mem.writeInt(u64, &version, predicate.expected_version, .little);
                var hash = std.crypto.hash.sha2.Sha256.init(.{});
                hash.update(&version);
                hash.update(&digest);
                out[0] = 4;
                hash.final(out[1..33]);
                return out;
            }
            std.mem.writeInt(u64, out[0..8], predicate.expected_version, .little);
            return out[0..8];
        },
        .exact_value => {
            out[0] = if (predicate.expected_value != null) 3 else 2;
            var hash = std.crypto.hash.Blake3.init(.{});
            const bytes = predicate.expected_value orelse "";
            var offset: usize = 0;
            while (offset < bytes.len) {
                if (control) |owner| try owner.checkpoint();
                const end = offset + @min(bytes.len - offset, 4096);
                hash.update(bytes[offset..end]);
                offset = end;
            }
            hash.final(out[1..33]);
            if (control) |owner| try owner.checkpoint();
            return out;
        },
    }
}

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
    intents_resolved: bool = false,
    intents_resolved_known: bool = false,
};

pub const TxnSummaryPage = struct {
    items: []TxnSummary,
    next_after: ?TxnId,
};

pub const ResolutionExtraBatch = struct {
    /// Borrowed through this resolution attempt. Terminal-decision retries do
    /// not attach it: they must never re-certify already applied row effects.
    commit_participant: ?@import("commit_participant.zig").Participant = null,
    /// Recovery owns one budgeted snapshot through atomic resolution.
    captured_intents: ?[]backend_scan.OwnedKVPair = null,
    cleanup_context: ?*anyopaque = null,
    writes: []const docstore.KVPair = &.{},
    deletes: []const []const u8 = &.{},
    /// The caller has already materialized every intent into `writes` and
    /// `deletes`. Intent records and locks are still retired atomically, but
    /// their legacy primary-key mutations must not be applied a second time.
    /// This is the allocation-free common path for relational commits.
    skip_all_intent_application: bool = false,
    /// Selective counterpart used by recovery batches that materialize only a
    /// subset of intents. Resolution indexes these borrowed keys once, keeping
    /// filtering linear instead of scanning this list for every intent.
    skip_intent_keys: []const []const u8 = &.{},
    /// Completion writes are safe and required even when the transaction
    /// decision was already durable. Raft entry markers belong here; replay and
    /// derived mutations remain in `writes` so terminal retries cannot reapply
    /// them over newer document state.
    completion_writes: []const docstore.KVPair = &.{},
    completion_deletes: []const []const u8 = &.{},
    resolved_participant: ?[]const u8 = null,
    /// Local coordinator self-ack waits for its durable session response.
    defer_retained_coordinator_acknowledgement: bool = false,
    replay: ?ReplayAppend = null,
    expected_intent_revision: ?u64 = null,
    /// Exact user keys captured from a validated intent snapshot. Ordinary
    /// commits can point-read these keys instead of prefix-scanning (and
    /// cloning) the whole mutable memtable. Recovery leaves this null.
    known_intent_keys: ?[]const []const u8 = null,
};

/// Additional primary-store mutations that must commit atomically with a
/// transaction metadata transition. Raft apply uses this to fence the exact log
/// entry without a crash window between the transaction and its replay marker.
pub const MutationExtraBatch = struct {
    writes: []const docstore.KVPair = &.{},
    deletes: []const []const u8 = &.{},
    /// DB preparation supplies a binding (including explicit schemaless).
    /// Generic transaction-manager callers cannot infer a table's schema.
    schema_binding: ?SchemaBinding = null,
    /// Zero disables admission only for low-level transaction-manager users.
    /// DB callers supply the replicated catalog policy, never local capacity.
    max_intent_admission_bytes: u64 = 0,
    preparation_allocator: ?Allocator = null,
    /// Exact old/new index tuple spans derived under the pinned index plan.
    /// Each prepared primary writer reserves its spans before publication.
    index_span_digests: []const [range_protection.index_span_digest_bytes]u8 = &.{},
};

pub const SchemaBinding = struct {
    // null explicitly binds a schemaless document table, not the future tip.
    version: ?u32 = null,
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

pub const ReplicationOutbox = struct {
    batch_payload: ?[]u8 = null,
    replay_payload: ?[]u8 = null,

    pub fn deinit(self: *ReplicationOutbox, alloc: Allocator) void {
        if (self.batch_payload) |payload| alloc.free(payload);
        if (self.replay_payload) |payload| alloc.free(payload);
        self.* = undefined;
    }
};

/// Owns the decoded slab through effect materialization and atomic resolution.
/// Existing per-field PrecomputedGeneratedBatch.deinit must not free its slices.
pub const PreparedEffects = struct {
    decoded: prepared_effects.Decoded,
    fingerprint: [32]u8,
    pub fn deinit(self: *@This()) void {
        self.decoded.deinit();
        self.* = undefined;
    }
};

/// Accepted effects and the exact owned input revision used to validate their
/// contract. Resolution must consume this batch rather than recollecting inputs
/// after validation. The caller retains apply/catalog/route ownership separately.
pub const PreparedResolutionInputs = struct {
    effects: PreparedEffects,
    inputs: PreparedIntentSnapshot,

    pub fn deinit(self: *@This()) void {
        self.effects.deinit();
        self.inputs.deinit();
        self.* = undefined;
    }
};

pub const ReplicationOutboxKind = enum { batch, replay };

pub const ReplayAppend = struct {
    sequence: u64,
    payload: []const u8,
};

pub const IntentBatch = struct {
    writes: []docstore.KVPair = &.{},
    deletes: [][]const u8 = &.{},
    revision: u64 = 0,
    schema_binding: ?SchemaBinding = null,
    prepared_rows: []?[]const u8 = &.{},
    // Primary slices borrow this owned snapshot. Do not duplicate every
    // payload merely to strip the one-byte intent envelope.
    owned_entries: ?[]backend_scan.OwnedKVPair = null,

    pub fn deinit(self: *IntentBatch, alloc: Allocator) void {
        if (self.prepared_rows.len != 0) alloc.free(self.prepared_rows);
        if (self.owned_entries) |entries| {
            backend_scan.freeResults(alloc, entries);
            alloc.free(self.writes);
            alloc.free(self.deletes);
            self.* = undefined;
            return;
        }
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

/// The producer consumes these owned intent bytes, rather than collecting a
/// second batch after capturing a digest. Retain the DB's catalog/route leases
/// separately and revalidate under apply ownership before publishing any seal.
pub const PreparedIntentSnapshot = struct {
    allocator: Allocator,
    contract: prepared_effects.Contract,
    batch: IntentBatch,

    pub fn deinit(self: *@This()) void {
        self.batch.deinit(self.allocator);
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
    /// Set atomically with the terminal record and intent deletion. Recovery
    /// can then distinguish a completed local resolution from an older or
    /// externally decided terminal record whose intents still need replay.
    intents_resolved: bool = false,
    intents_resolved_known: bool = false,
    /// Agreed at BEGIN, retained in the same record through resolution and
    /// the existing retry window. Absence never upgrades a legacy contract.
    preparation_requirement: ?prepared_effects.Requirement = null,
    preparation_binding: ?prepared_effects.Binding = null,

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
const txn_record_v6_size = 53;
const txn_record_v7_size = 54;
const txn_record_v8_size = txn_record_v7_size + prepared_effects.Binding.encoded_bytes;

// ============================================================================
// TxnManager
// ============================================================================

/// Apply-fenced semantic reads must not interpret physical bytes protected
/// by another transaction's unresolved exclusive intent. Shared read guards
/// remain compatible. Reuse one lock-key buffer and the caller's point probe;
/// no transaction-record scan or mutable-store snapshot is needed.
pub const IntentReadGuard = struct {
    alloc: Allocator,
    exclude_txn: ?TxnId,
    lock_key: std.ArrayList(u8) = .empty,

    pub fn init(alloc: Allocator, exclude_txn: ?TxnId) IntentReadGuard {
        return .{ .alloc = alloc, .exclude_txn = exclude_txn };
    }

    pub fn deinit(self: *IntentReadGuard) void {
        self.lock_key.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn check(self: *IntentReadGuard, view: anytype, user_key: []const u8) !void {
        self.lock_key.clearRetainingCapacity();
        try self.lock_key.appendSlice(self.alloc, intent_locks_prefix);
        try self.lock_key.appendSlice(self.alloc, user_key);
        const owner = view.get(self.lock_key.items) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        if (owner.len != @sizeOf(TxnId)) return TxnError.InvalidTxnRecord;
        if (self.exclude_txn) |txn| if (std.mem.eql(u8, owner, &txn)) return;
        return TxnError.IntentConflict;
    }
};

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
        try self.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(
            txn_id,
            timestamp,
            created_at,
            participants,
            coordinator,
            retain_terminal,
            .{},
        );
    }

    pub fn initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(self: *TxnManager, txn_id: TxnId, timestamp: u64, created_at: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, extra_batch: MutationExtraBatch) !void {
        return self.initTransactionWithParticipantsCreatedAtRoleAndRetentionInner(txn_id, timestamp, created_at, participants, coordinator, retain_terminal, extra_batch, null, null, null);
    }

    pub fn initTransactionWithParticipantsCreatedAtRoleAndRetentionWithAdmission(self: *TxnManager, txn_id: TxnId, timestamp: u64, created_at: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, control: ?read_control.Port) !void {
        return self.initTransactionWithParticipantsCreatedAtRoleAndRetentionInner(txn_id, timestamp, created_at, participants, coordinator, retain_terminal, .{}, control, null, null);
    }

    /// Opt-in BEGIN contract. Caller retains the DB apply fence and passes
    /// retained controls while holding dependencies. A legacy in-flight record
    /// cannot be upgraded after its coordinator may already have decided.
    pub fn initTransactionRequiringPreparedEffects(self: *TxnManager, txn_id: TxnId, timestamp: u64, created_at: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, requirement: prepared_effects.Requirement, extra_batch: MutationExtraBatch, original: ?read_control.Port) !void {
        return self.initTransactionRequiringPreparedEffectsWithBinding(txn_id, timestamp, created_at, participants, coordinator, retain_terminal, requirement, null, extra_batch, original);
    }

    pub fn initTransactionRequiringPreparedEffectsWithBinding(self: *TxnManager, txn_id: TxnId, timestamp: u64, created_at: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, requirement: prepared_effects.Requirement, binding: ?prepared_effects.Binding, extra_batch: MutationExtraBatch, original: ?read_control.Port) !void {
        if (binding) |value| _ = try value.encode();
        var trace: BeginAdmissionTrace = .{};
        self.initRequiredPreparationInner(txn_id, timestamp, created_at, participants, coordinator, retain_terminal, requirement, binding, extra_batch, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
    }

    fn initRequiredPreparationInner(self: *TxnManager, txn_id: TxnId, timestamp: u64, created_at: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, requirement: prepared_effects.Requirement, binding: ?prepared_effects.Binding, extra_batch: MutationExtraBatch, control: ?read_control.Port) !void {
        if (control) |owner| try owner.checkpoint();
        const existing = self.loadTransactionRecordWithAdmission(txn_id, control) catch |err| blk: {
            if (err != TxnError.TxnNotFound) return err;
            if (control) |owner| try owner.checkpoint();
            break :blk null;
        };
        if (existing) |record| if (record.status != .pending) return TxnError.DecisionConflict;
        const previous = try self.loadPreparationRequirementInner(txn_id, control);
        const key = makePreparationRequirementKey(txn_id);
        const effect_key = makePreparedEffectsKey(txn_id);
        const record_key = makeRecordKey(txn_id);
        for (extra_batch.writes) |write| {
            if (control) |owner| try owner.checkpoint();
            if (std.mem.eql(u8, write.key, &key) or std.mem.eql(u8, write.key, &effect_key) or std.mem.eql(u8, write.key, &record_key)) return error.InvalidPreparationRequirement;
        }
        for (extra_batch.deletes) |deleted| {
            if (control) |owner| try owner.checkpoint();
            if (std.mem.eql(u8, deleted, &key) or std.mem.eql(u8, deleted, &effect_key) or std.mem.eql(u8, deleted, &record_key)) return error.InvalidPreparationRequirement;
        }
        if (existing != null) {
            const accepted = previous orelse return error.PreparationContractConflict;
            if (accepted.mode != requirement.mode) return error.PreparationContractConflict;
            // Retry never rewrites the requirement or reacquires its credit.
            return self.initTransactionWithParticipantsCreatedAtRoleAndRetentionInner(txn_id, timestamp, created_at, participants, coordinator, retain_terminal, extra_batch, control, requirement, binding);
        }
        if (previous != null or try self.hasPreparedEffects(txn_id)) return error.InvalidTxnRecord;
        const encoded = requirement.encode();
        var writes: std.ArrayListUnmanaged(docstore.KVPair) = .empty;
        defer writes.deinit(self.alloc);
        try writes.append(self.alloc, .{ .key = &key, .value = &encoded });
        try writes.appendSlice(self.alloc, extra_batch.writes);
        var batch = extra_batch;
        batch.writes = writes.items;
        try self.initTransactionWithParticipantsCreatedAtRoleAndRetentionInner(txn_id, timestamp, created_at, participants, coordinator, retain_terminal, batch, control, requirement, binding);
    }

    pub fn loadPreparationRequirement(self: *TxnManager, txn_id: TxnId, original: ?read_control.Port) !?prepared_effects.Requirement {
        var trace: MutationAdmissionTrace = .{};
        const result = self.loadPreparationRequirementInner(txn_id, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
        return result;
    }

    pub const PreparationDecision = struct { status: TxnStatus, requirement: ?prepared_effects.Requirement, binding: ?prepared_effects.Binding };

    /// Durable evidence for requirement-aware retries, including after accepted
    /// effects retire. Legacy records never gain authority from a sidecar alone.
    pub fn getPreparationDecision(self: *TxnManager, txn_id: TxnId, original: ?read_control.Port) !PreparationDecision {
        var trace: MutationAdmissionTrace = .{};
        const control = trace.bind(original);
        const record = self.loadTransactionRecordWithAdmission(txn_id, control) catch |err| return trace.failure orelse err;
        if (record.status != .aborted and record.preparation_requirement != null) {
            const sidecar = self.loadPreparationRequirementInner(txn_id, control) catch |err| return trace.failure orelse err;
            if (trace.failure) |cause| return cause;
            if (record.intents_resolved_known and record.intents_resolved) {
                if (sidecar != null) return error.InvalidTxnRecord;
            } else {
                const accepted = sidecar orelse return error.PreparedEffectsRequired;
                if (accepted.mode != record.preparation_requirement.?.mode) return error.PreparationContractConflict;
            }
        }
        if (control) |owner| try owner.checkpoint();
        if (trace.failure) |cause| return cause;
        return .{ .status = record.status, .requirement = record.preparation_requirement, .binding = record.preparation_binding };
    }

    pub fn getBoundPreparedSealReceipt(self: *TxnManager, txn_id: TxnId, expected: prepared_effects.Binding, original: ?read_control.Port) !prepared_effects.BoundReceipt {
        var trace: MutationAdmissionTrace = .{};
        const control = trace.bind(original);
        const decision = self.getPreparationDecision(txn_id, control) catch |err| return trace.failure orelse err;
        if (decision.status == .aborted) return error.DecisionConflict;
        const binding = decision.binding orelse return error.PreparationContractConflict;
        if (!std.meta.eql(binding, expected)) return error.PreparationContractConflict;
        const required = decision.requirement orelse return error.PreparationContractConflict;
        const key = makePreparedEffectsKey(txn_id);
        const raw = self.getAllocWithAdmission(self.alloc, &key, control) catch |err| {
            if (trace.failure) |cause| return cause;
            return if (err == error.NotFound) error.PreparedEffectsRequired else err;
        };
        defer self.alloc.free(raw);
        const inspection = prepared_effects.inspect(raw, control) catch |err| return trace.failure orelse err;
        const contract = inspection.contract;
        const record = self.loadTransactionRecordWithAdmission(txn_id, control) catch |err| return trace.failure orelse err;
        if (!std.meta.eql(record.preparation_binding, decision.binding) or record.preparation_requirement == null or record.preparation_requirement.?.mode != required.mode or record.status == .aborted) return error.PreparationContractConflict;
        if (!record.prepared or (record.intents_resolved_known and record.intents_resolved)) return error.InvalidTxnRecord;
        if (!std.mem.eql(u8, &contract.txn_id, &txn_id) or contract.mode != required.mode or contract.intent_revision != record.intent_revision) return error.PreparedEffectsMismatch;
        if (control) |owner| try owner.checkpoint();
        if (trace.failure) |cause| return cause;
        return .{ .txn_id = txn_id, .requirement = required, .binding = binding, .intent_revision = contract.intent_revision, .intent_digest = contract.intent_digest, .catalog_digest = contract.catalog_digest, .frame_digest = inspection.fingerprint };
    }

    fn loadPreparationRequirementInner(self: *TxnManager, txn_id: TxnId, control: ?read_control.Port) !?prepared_effects.Requirement {
        const key = makePreparationRequirementKey(txn_id);
        const raw = self.getAllocWithAdmission(self.alloc, &key, control) catch |err| {
            if (err != error.NotFound) return err;
            if (control) |owner| try owner.checkpoint();
            return null;
        };
        defer self.alloc.free(raw);
        return try prepared_effects.Requirement.decode(raw);
    }

    pub fn hasPreparationRequirement(self: *TxnManager, txn_id: TxnId) !bool {
        const key = makePreparationRequirementKey(txn_id);
        return self.keyExists(&key);
    }

    fn initTransactionWithParticipantsCreatedAtRoleAndRetentionInner(
        self: *TxnManager,
        txn_id: TxnId,
        timestamp: u64,
        created_at: u64,
        participants: []const []const u8,
        coordinator: bool,
        retain_terminal: bool,
        extra_batch: MutationExtraBatch,
        original_control: ?read_control.Port,
        preparation_requirement: ?prepared_effects.Requirement,
        preparation_binding: ?prepared_effects.Binding,
    ) !void {
        var trace: BeginAdmissionTrace = .{};
        const control = trace.bind(original_control);
        if (control) |owner| try owner.checkpoint();
        const key = makeRecordKey(txn_id);
        const existing = self.loadTransactionRecordWithAdmission(txn_id, control) catch |err| blk: {
            // A caller rejection may share the store's missing-record name.
            // It must never authorize creating a new transaction instead.
            if (trace.failure) |cause| return cause;
            break :blk switch (err) {
                TxnError.TxnNotFound => null,
                else => return err,
            };
        };
        if (existing) |record| {
            if (!std.meta.eql(record.preparation_binding, preparation_binding)) return error.PreparationContractConflict;
            if (preparation_requirement) |required| {
                const accepted = record.preparation_requirement orelse return error.PreparationContractConflict;
                if (accepted.mode != required.mode) return error.PreparationContractConflict;
            }
            if (record.status != .pending or record.begin_timestamp != timestamp) return TxnError.DecisionConflict;
            if (record.coordinator_known and record.coordinator != coordinator) return TxnError.DecisionConflict;
            if (record.retain_terminal_known and record.retain_terminal != retain_terminal) return TxnError.DecisionConflict;
            const persisted = self.loadParticipantSetWithAdmission(self.alloc, participants_prefix, txn_id, control) catch |err| return trace.failure orelse err;
            defer freeParticipantList(self.alloc, persisted);
            if (!try participantListsEqualWithAdmission(persisted, participants, control)) return TxnError.DecisionConflict;
            // An idempotent begin from a new binary is the authoritative point
            // where a legacy record can safely learn its coordinator role.
            // Persist the upgrade before prepare can rewrite the record as v4.
            if (!record.coordinator_known or !record.retain_terminal_known) {
                var upgraded = record;
                upgraded.coordinator = coordinator;
                upgraded.coordinator_known = true;
                upgraded.retain_terminal = retain_terminal;
                upgraded.retain_terminal_known = true;
                const upgraded_value = try self.encodeRecord(upgraded);
                defer self.alloc.free(upgraded_value);
                var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
                defer writes.deinit(self.alloc);
                try writes.append(self.alloc, .{ .key = &key, .value = upgraded_value });
                try writes.appendSlice(self.alloc, extra_batch.writes);
                if (control) |owner| try owner.checkpoint();
                try self.applyBatchWithAdmission(writes.items, extra_batch.deletes, null, control);
            } else {
                if (control) |owner| try owner.checkpoint();
                try self.applyMutationExtraBatchWithAdmission(extra_batch, control);
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
            .preparation_requirement = preparation_requirement,
            .preparation_binding = preparation_binding,
        };
        const record_value = try self.encodeRecord(record);
        defer self.alloc.free(record_value);
        const participant_key = makeSidecarKey(participants_prefix, txn_id);
        const resolved_key = makeSidecarKey(resolved_participants_prefix, txn_id);
        const participant_value = if (participants.len > 0) try encodeParticipantListWithAdmission(self.alloc, participants, control) else null;
        defer if (participant_value) |value| self.alloc.free(value);
        var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
        defer writes.deinit(self.alloc);
        try writes.append(self.alloc, .{ .key = &key, .value = record_value });
        if (participant_value) |value| {
            try writes.append(self.alloc, .{ .key = &participant_key, .value = value });
        }
        try writes.appendSlice(self.alloc, extra_batch.writes);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);
        if (participants.len > 0) {
            try deletes.append(self.alloc, &resolved_key);
        } else {
            try deletes.appendSlice(self.alloc, &.{ &participant_key, &resolved_key });
        }
        try deletes.appendSlice(self.alloc, extra_batch.deletes);
        if (control) |owner| try owner.checkpoint();
        try self.applyBatchWithAdmission(writes.items, deletes.items, null, control);
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
        try self.writeIntentsExtraBatch(txn_id, intents, predicates, .{});
    }

    pub fn writeIntentsExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        intents: []const WriteIntent,
        predicates: []const VersionPredicate,
        extra_batch: MutationExtraBatch,
    ) !void {
        return self.writeIntentsExtraBatchInner(txn_id, intents, predicates, extra_batch, null);
    }

    pub fn writeIntentsExtraBatchWithAdmission(self: *TxnManager, txn_id: TxnId, intents: []const WriteIntent, predicates: []const VersionPredicate, extra_batch: MutationExtraBatch, original: ?read_control.Port) !void {
        var trace: MutationAdmissionTrace = .{};
        return self.writeIntentsExtraBatchInner(txn_id, intents, predicates, extra_batch, trace.bind(original)) catch |err| return trace.failure orelse err;
    }

    fn writeIntentsExtraBatchInner(
        self: *TxnManager,
        txn_id: TxnId,
        intents: []const WriteIntent,
        predicates: []const VersionPredicate,
        extra_batch: MutationExtraBatch,
        control: ?read_control.Port,
    ) !void {
        if (control) |owner| try owner.checkpoint();
        if (extra_batch.index_span_digests.len > max_read_guards_per_transaction) return error.TransactionTooLarge;
        for (intents) |intent| {
            if (control) |owner| try owner.checkpoint();
            if (intent.json_null_fields.len != 0 and intent.prepared_row == null) return error.PreparedIntentRequiresMaterialization;
        }
        var record = try self.loadTransactionRecordWithAdmission(txn_id, control);
        if (record.status != .pending) return TxnError.DecisionConflict;
        if (try self.hasPreparedEffects(txn_id)) return error.PreparedEffectsSealed;

        const schema_lease_key = makeSidecarKey(schema_leases_prefix, txn_id);
        var schema_lease_value: [5]u8 = @splat(0);
        const binding = extra_batch.schema_binding orelse SchemaBinding{};
        schema_lease_value[0] = @intFromBool(binding.version != null);
        if (binding.version) |version|
            std.mem.writeInt(u32, schema_lease_value[1..5], version, .little);
        const previous_lease = self.getAllocWithAdmission(self.alloc, &schema_lease_key, control) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        defer if (previous_lease) |value| self.alloc.free(value);
        if (previous_lease) |value| if (extra_batch.schema_binding != null and !std.mem.eql(u8, value, &schema_lease_value))
            return error.SchemaInUse;

        // Emit CheckPredicates before checks: TLA+ spec models this as an
        // always-succeeding snapshot step; WriteIntentFails detects conflicts.
        if (self.trace_writer) |tw| {
            tw.traceEvent(&.{
                .name = "CheckPredicates",
                .txn_id = txn_id,
                .shard_id = self.shard_id,
            });
        }

        if (control) |owner| try owner.checkpoint();
        self.checkVersionPredicatesWithAdmission(predicates, txn_id, control) catch |err| {
            self.traceWriteIntentFails(txn_id, intents, "VersionConflict");
            return err;
        };
        if (control) |owner| try owner.checkpoint();
        self.checkIntentConflictsWithAdmission(intents, txn_id, control) catch |err| {
            self.traceWriteIntentFails(txn_id, intents, "IntentConflict");
            return err;
        };
        if (control) |owner| try owner.checkpoint();
        try self.checkIndexSpanGuardConflictsWithAdmission(extra_batch.index_span_digests, txn_id, control);

        if (control) |owner| try owner.checkpoint();
        const previous_admission = try self.loadIntentAdmissionWithAdmission(self.alloc, txn_id, control);
        var admission = previous_admission orelse IntentAdmission{};
        var pending_costs = std.StringHashMapUnmanaged(IntentCost).empty;
        defer pending_costs.deinit(self.alloc);
        var last_intents = std.StringHashMapUnmanaged(usize).empty;
        defer last_intents.deinit(self.alloc);

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

        // Only transactions from released document-storage versions need this
        // one-time conversion. New prepares never reread their prior payloads
        // or rewrite a growing list of keys.
        if ((previous_admission == null or !previous_admission.?.retention_tracked) and record.intent_revision != 0) {
            admission = .{};
            var previous = try self.collectIntentBatchWithAdmission(self.alloc, txn_id, control);
            defer previous.deinit(self.alloc);
            for (previous.owned_entries.?) |entry| {
                if (control) |owner| try owner.checkpoint();
                const key = entry.key[intents_prefix.len + 17 ..];
                const decoded = try IntentValue.decode(entry.value);
                const previous_intent: WriteIntent = .{ .key = key, .value = decoded.value, .prepared_row = decoded.prepared_row };
                const cost = try intentAdmissionBytesWithAdmission(previous_intent, control);
                const retained = try retainedIntentBytesWithAdmission(self.alloc, previous_intent, control);
                admission.count += 1;
                admission.bytes = std.math.add(u64, admission.bytes, cost) catch return error.TransactionTooLarge;
                admission.retained_bytes = std.math.add(u64, admission.retained_bytes, retained) catch return error.TransactionTooLarge;
                admission.retained_keys += @intFromBool(retained != 0);
                try pending_costs.put(self.alloc, key, .{ .bytes = cost, .retained = retained, .retained_keys = @intFromBool(retained != 0) });
                const member_key = try makeIntentMemberKeyWithAdmission(self.alloc, txn_id, key, control);
                try appendOwnedBytes(self.alloc, &write_keys, member_key);
                const member_value = try self.alloc.alloc(u8, 16);
                try appendOwnedBytes(self.alloc, &write_vals, member_value);
                std.mem.writeInt(u64, member_value[0..8], cost, .little);
                std.mem.writeInt(u64, member_value[8..16], retained, .little);
                try writes.append(self.alloc, .{ .key = member_key, .value = member_value });
            }
            // Hash-map keys must outlive the snapshot.
            pending_costs.clearRetainingCapacity();
            for (writes.items) |write| try pending_costs.put(self.alloc, write.key[intent_members_prefix.len + 17 ..], .{ .bytes = std.mem.readInt(u64, write.value[0..8], .little), .retained = std.mem.readInt(u64, write.value[8..16], .little), .retained_keys = @intFromBool(std.mem.readInt(u64, write.value[8..16], .little) != 0) });
        }

        // Compute the final replacement-aware ledger before allocating any
        // payload envelopes. Repeated keys retain only their final mutation.
        for (intents, 0..) |intent, index| {
            if (control) |owner| try owner.checkpoint();
            const member_key = try makeIntentMemberKeyWithAdmission(self.alloc, txn_id, intent.key, control);
            defer self.alloc.free(member_key);
            const prior = if (pending_costs.get(intent.key)) |cost| cost else blk: {
                const raw = self.getAllocWithAdmission(self.alloc, member_key, control) catch |err| switch (err) {
                    error.NotFound => break :blk null,
                    else => return err,
                };
                defer self.alloc.free(raw);
                if (raw.len != 16 and raw.len != 24) return error.InvalidTxnRecord;
                break :blk IntentCost{ .bytes = std.mem.readInt(u64, raw[0..8], .little), .retained = std.mem.readInt(u64, raw[8..16], .little), .retained_keys = if (raw.len == 24) std.mem.readInt(u64, raw[16..24], .little) else @intFromBool(std.mem.readInt(u64, raw[8..16], .little) != 0) };
            };
            const cost = try intentAdmissionBytesWithAdmission(intent, control);
            const retained = try retainedIntentBytesWithAdmission(self.alloc, intent, control);
            const retained_keys = try retainedIntentKeys(intent, retained);
            if (prior) |old| {
                admission.bytes = std.math.sub(u64, admission.bytes, old.bytes) catch return error.InvalidTxnRecord;
                admission.retained_bytes = std.math.sub(u64, admission.retained_bytes, old.retained) catch return error.InvalidTxnRecord;
                admission.retained_keys = std.math.sub(u64, admission.retained_keys, old.retained_keys) catch return error.InvalidTxnRecord;
            } else {
                admission.count = std.math.add(u64, admission.count, 1) catch return error.TransactionTooLarge;
            }
            admission.bytes = std.math.add(u64, admission.bytes, cost) catch return error.TransactionTooLarge;
            admission.retained_bytes = std.math.add(u64, admission.retained_bytes, retained) catch return error.TransactionTooLarge;
            admission.retained_keys = std.math.add(u64, admission.retained_keys, retained_keys) catch return error.TransactionTooLarge;
            try pending_costs.put(self.alloc, intent.key, .{ .bytes = cost, .retained = retained, .retained_keys = retained_keys });
            try last_intents.put(self.alloc, intent.key, index);
        }
        const publication_credit_key = publication_backlog.transactionCreditKey(txn_id);
        var publication_credit: [8]u8 = undefined;
        if (admission.count != 0) {
            std.mem.writeInt(u64, &publication_credit, try publication_backlog.transactionBatchBudget(admission.bytes), .little);
            // Append before any new intent or vote, without moving the whole
            // mutation array after preparing it. Legacy conversion above only
            // stages member sidecars; its old intents still fence migration.
            try writes.append(self.alloc, .{ .key = &publication_credit_key, .value = &publication_credit });
        }
        var range_reservations = std.ArrayListUnmanaged(VersionPredicate).empty;
        defer range_reservations.deinit(self.alloc);
        try range_reservations.appendSlice(self.alloc, predicates);
        var writer_keys: [range_protection.bucket_count][range_protection.writer_prefix.len + 2]u8 = undefined;
        var writer_buckets: std.StaticBitSet(range_protection.bucket_count) = .empty;
        const index_writer_keys = try self.alloc.alloc([range_protection.index_writer_prefix.len + range_protection.index_span_digest_bytes]u8, extra_batch.index_span_digests.len);
        defer self.alloc.free(index_writer_keys);
        {
            var probe = if (control) |owner| try self.store.beginProbeWithAdmission(.retain, owner) else try self.store.beginProbe();
            defer probe.abort();
            const tracking_active = try range_protection.isActive(&probe);
            if (!tracking_active and extra_batch.index_span_digests.len != 0) return error.SqlRangeTrackingRequired;
            if (tracking_active) {
                for (intents) |intent| {
                    if (control) |owner| try owner.checkpoint();
                    if (std.mem.startsWith(u8, intent.key, "\x00\x00")) continue;
                    const id = range_protection.bucket(intent.key);
                    if (writer_buckets.isSet(id)) continue;
                    writer_buckets.set(id);
                    writer_keys[id] = range_protection.writerKey(id);
                    try range_reservations.append(self.alloc, .{ .key = &writer_keys[id], .expected_version = 0, .comparison = .exact_value, .expected_value = null });
                }
                for (extra_batch.index_span_digests, 0..) |digest, i| {
                    if (control) |owner| try owner.checkpoint();
                    index_writer_keys[i] = range_protection.indexWriterKey(digest);
                    try range_reservations.append(self.alloc, .{ .key = &index_writer_keys[i], .expected_version = 0, .comparison = .exact_value, .expected_value = null });
                }
            }
        }
        if (control) |owner| try owner.checkpoint();
        const read_admission = try self.stageReadGuards(txn_id, range_reservations.items, &last_intents, &write_keys, &write_vals, &writes, control);
        const total_bytes = std.math.add(u64, admission.bytes, read_admission.next.bytes) catch return error.TransactionTooLarge;
        const previous_bytes = std.math.add(u64, if (previous_admission) |previous| previous.bytes else 0, read_admission.previous.bytes) catch return error.InvalidTxnRecord;
        if (extra_batch.max_intent_admission_bytes != 0 and total_bytes > extra_batch.max_intent_admission_bytes and
            total_bytes > previous_bytes)
            return error.TransactionTooLarge;

        for (intents, 0..) |intent, index| {
            if (control) |owner| try owner.checkpoint();
            if (last_intents.get(intent.key).? != index) continue;
            const member_key = try makeIntentMemberKeyWithAdmission(self.alloc, txn_id, intent.key, control);
            try appendOwnedBytes(self.alloc, &write_keys, member_key);
            const member_value = try self.alloc.alloc(u8, 24);
            std.mem.writeInt(u64, member_value[0..8], pending_costs.get(intent.key).?.bytes, .little);
            std.mem.writeInt(u64, member_value[8..16], pending_costs.get(intent.key).?.retained, .little);
            std.mem.writeInt(u64, member_value[16..24], pending_costs.get(intent.key).?.retained_keys, .little);
            try appendOwnedBytes(self.alloc, &write_vals, member_value);
            try writes.append(self.alloc, .{ .key = member_key, .value = member_value });
            const intent_key = try self.makeIntentKeyWithAdmission(txn_id, intent.key, control);
            try appendOwnedBytes(self.alloc, &write_keys, intent_key);

            const val = try @import("transaction_intent_admission.zig").encode(self.alloc, intent, control);
            try appendOwnedBytes(self.alloc, &write_vals, val);

            try writes.append(self.alloc, .{ .key = intent_key, .value = val });

            const lock_key = try self.makeIntentLockKeyWithAdmission(intent.key, control);
            try appendOwnedBytes(self.alloc, &write_keys, lock_key);
            const owner = try self.alloc.dupe(u8, &txn_id);
            try appendOwnedBytes(self.alloc, &write_vals, owner);
            try writes.append(self.alloc, .{ .key = lock_key, .value = owner });
        }
        record.intent_revision = std.math.add(u64, record.intent_revision, 1) catch return error.TransactionRevisionOverflow;
        // Publish the prepare vote in the same backend batch as the intents.
        // Recovery can therefore never observe intents without the durable
        // fencing bit that prevents presumed abort.
        record.prepared = true;
        const record_key = makeRecordKey(txn_id);
        const record_value = try self.encodeRecord(record);
        try appendOwnedBytes(self.alloc, &write_vals, record_value);
        try writes.append(self.alloc, .{ .key = &record_key, .value = record_value });

        const intent_keys_key = makeSidecarKey(intent_admission_prefix, txn_id);
        const intent_keys_value = try self.alloc.alloc(u8, 32);
        std.mem.writeInt(u64, intent_keys_value[0..8], admission.count, .little);
        std.mem.writeInt(u64, intent_keys_value[8..16], admission.bytes, .little);
        std.mem.writeInt(u64, intent_keys_value[16..24], admission.retained_bytes, .little);
        std.mem.writeInt(u64, intent_keys_value[24..32], admission.retained_keys, .little);
        try appendOwnedBytes(self.alloc, &write_vals, intent_keys_value);
        try writes.append(self.alloc, .{ .key = &intent_keys_key, .value = intent_keys_value });
        if (extra_batch.schema_binding != null)
            try writes.append(self.alloc, .{ .key = &schema_lease_key, .value = &schema_lease_value });

        try writes.appendSlice(self.alloc, extra_batch.writes);

        try self.applyBatchWithParticipantAndAdmission(writes.items, extra_batch.deletes, null, .{ .previous = if (previous_admission) |old| old.reservation() else 0, .next = admission.reservation() }, null, control);

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
        return self.resolveIntentsWithPreparedEffects(txn_id, status, timestamp, extra_batch, null, null);
    }

    /// The trusted materializer supplies this owner only after staging all its
    /// effects in extra_batch. No producer is invoked by this method.
    pub fn resolvePreparedIntents(self: *TxnManager, txn_id: TxnId, timestamp: u64, plan: *const PreparedEffects, extra_batch: ResolutionExtraBatch, original: ?read_control.Port) !ResolutionOutcome {
        const contract = plan.decoded.plan.contract;
        if (!std.mem.eql(u8, &contract.txn_id, &txn_id)) return error.PreparedEffectsMismatch;
        var batch = extra_batch;
        if (batch.expected_intent_revision) |revision| if (revision != contract.intent_revision) return error.IntentSnapshotChanged;
        batch.expected_intent_revision = contract.intent_revision;
        var trace: MutationAdmissionTrace = .{};
        return self.resolveIntentsWithPreparedEffects(txn_id, .committed, timestamp, batch, &plan.fingerprint, trace.bind(original)) catch |err| return trace.failure orelse err;
    }

    fn resolveIntentsWithPreparedEffects(self: *TxnManager, txn_id: TxnId, status: TxnStatus, timestamp: u64, extra_batch: ResolutionExtraBatch, consumed: ?*const [32]u8, control: ?read_control.Port) !ResolutionOutcome {
        if (control) |owner| try owner.checkpoint();
        const rec_key = makeRecordKey(txn_id);
        const schema_lease_key = makeSidecarKey(schema_leases_prefix, txn_id);
        var record = try self.loadTransactionRecord(txn_id);
        if (record.status != .pending and record.status != status) {
            self.traceResolveDecisionConflict(txn_id, record.status, status, timestamp);
            return TxnError.DecisionConflict;
        }
        const prepared_key = makePreparedEffectsKey(txn_id);
        const requirement_key = makePreparationRequirementKey(txn_id);
        // Abort can retire opaque/corrupt prepared bytes. Commit must never
        // silently fall back to legacy reconstruction when a sealed plan exists.
        if (status == .committed) {
            const requirement = try self.loadPreparationRequirementInner(txn_id, control);
            if (!(record.intents_resolved_known and record.intents_resolved)) {
                if (record.preparation_requirement) |agreed| {
                    const accepted = requirement orelse return error.PreparedEffectsRequired;
                    if (accepted.mode != agreed.mode) return error.PreparationContractConflict;
                }
            }
            if (requirement != null and record.intents_resolved_known and record.intents_resolved) return error.InvalidTxnRecord;
            if (try self.hasPreparedEffects(txn_id)) {
                if (record.intents_resolved_known and record.intents_resolved) return error.InvalidTxnRecord;
                const accepted = consumed orelse return error.PreparedEffectsRequired;
                const raw = try self.getAllocWithAdmission(self.alloc, &prepared_key, control);
                defer self.alloc.free(raw);
                const inspection = try prepared_effects.inspect(raw, control);
                if (requirement) |required| if (required.mode != inspection.contract.mode) return error.PreparationContractConflict;
                if (inspection.contract.intent_revision != record.intent_revision or
                    !std.mem.eql(u8, &inspection.contract.txn_id, &txn_id) or
                    !std.mem.eql(u8, &inspection.fingerprint, accepted)) return error.PreparedEffectsMismatch;
            } else if (requirement != null or (consumed != null and !(record.status == .committed and record.intents_resolved_known and record.intents_resolved))) return error.PreparedEffectsRequired;
        }
        var resolved_participant_key: [resolved_participants_prefix.len + 16]u8 = undefined;
        var resolved_participant_value: ?[]u8 = null;
        defer if (resolved_participant_value) |value| self.alloc.free(value);
        var indexed_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer indexed_arena.deinit();
        var indexed_writes: std.ArrayListUnmanaged(docstore.KVPair) = .empty;
        var indexed_deletes: std.ArrayListUnmanaged([]const u8) = .empty;
        const defer_ack = extra_batch.defer_retained_coordinator_acknowledgement and status == .committed and record.coordinator and record.retain_terminal;
        if (if (defer_ack) null else extra_batch.resolved_participant) |participant| {
            const index_key = makeSidecarKey(participant_index_prefix, txn_id);
            if (try self.keyExists(&index_key)) {
                try self.stageParticipantResolutions(txn_id, &.{participant}, indexed_arena.allocator(), &indexed_writes, &indexed_deletes);
            } else {
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
                var already_resolved = false;
                for (resolved) |existing| {
                    if (std.mem.eql(u8, existing, participant)) {
                        already_resolved = true;
                        break;
                    }
                }
                if (!already_resolved) {
                    const next = try self.alloc.alloc([]const u8, resolved.len + 1);
                    defer self.alloc.free(next);
                    for (resolved, 0..) |existing, i| next[i] = existing;
                    next[resolved.len] = participant;
                    resolved_participant_key = makeSidecarKey(resolved_participants_prefix, txn_id);
                    resolved_participant_value = try encodeParticipantList(self.alloc, next);
                }
            }
        }
        if (extra_batch.expected_intent_revision) |expected| {
            if (record.intent_revision != expected) return error.IntentSnapshotChanged;
        }
        const was_terminal = record.status != .pending;
        applyResolveDecision(&record, status, timestamp) catch |err| {
            if (err == TxnError.DecisionConflict) {
                self.traceResolveDecisionConflict(txn_id, record.status, status, timestamp);
            }
            return err;
        };
        if (was_terminal and record.intents_resolved_known and record.intents_resolved) {
            var completion_writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
            defer completion_writes.deinit(self.alloc);
            try completion_writes.appendSlice(self.alloc, extra_batch.completion_writes);
            if (resolved_participant_value) |value| {
                try completion_writes.append(self.alloc, .{ .key = &resolved_participant_key, .value = value });
            }
            try completion_writes.appendSlice(self.alloc, indexed_writes.items);
            var completion_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer completion_deletes.deinit(self.alloc);
            try completion_deletes.appendSlice(self.alloc, extra_batch.completion_deletes);
            try completion_deletes.appendSlice(self.alloc, indexed_deletes.items);
            if (status == .aborted and try self.hasPreparedEffects(txn_id)) try completion_deletes.append(self.alloc, &prepared_key);
            if (status == .aborted and try self.hasPreparationRequirement(txn_id)) try completion_deletes.append(self.alloc, &requirement_key);
            if (completion_writes.items.len != 0 or completion_deletes.items.len != 0) {
                try self.applyResolutionResourceBatch(completion_writes.items, completion_deletes.items, null, null, null, control, if (status == .aborted) .{ .id = txn_id, .status = status } else null);
            }
            return .{
                .applied = false,
                .replay_sequence = record.replay_sequence,
            };
        }

        // Scan all intents for this txn
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        if (extra_batch.captured_intents != null and extra_batch.expected_intent_revision == null) return error.InvalidArgument;
        const intent_entries = if (extra_batch.captured_intents) |entries| entries else if (extra_batch.known_intent_keys) |keys|
            if (extra_batch.skip_all_intent_application and extra_batch.expected_intent_revision != null)
                try intentRetirementEntries(self.alloc, scan_prefix, keys)
            else
                try self.loadIntentEntriesByKeys(self.alloc, scan_prefix, keys)
        else if (status == .aborted)
            try self.loadIntentRetirementEntries(self.alloc, txn_id, scan_prefix)
        else
            try self.loadIntentEntries(self.alloc, txn_id, scan_prefix);
        defer if (extra_batch.captured_intents == null) backend_scan.freeResults(self.alloc, intent_entries);

        const read_prefix = makeSidecarKey(read_members_prefix, txn_id);
        const read_admission = try self.loadReadAdmission(txn_id);
        const read_entries: []backend_scan.OwnedKVPair = if (read_admission.count != 0) try self.scanPrefix(self.alloc, &read_prefix) else &.{};
        defer backend_scan.freeResults(self.alloc, read_entries);
        if (read_entries.len != read_admission.count) return error.InvalidTxnRecord;

        // A resolve retry after the terminal record and all intents are already
        // durable must not apply the caller's derived batch again. In
        // particular, doing so could overwrite a newer user write with the
        // transaction's old value.
        // An accepted effect plan can owe output without any row intents.
        // Only the atomic plan-consuming batch proves that work completed.
        if (was_terminal and consumed == null and intent_entries.len == 0 and read_entries.len == 0) {
            record.intents_resolved = true;
            record.intents_resolved_known = true;
            const marker_value = try self.encodeRecord(record);
            defer self.alloc.free(marker_value);
            const stale_manifest_key = makeSidecarKey(intent_keys_prefix, txn_id);
            const stale_admission_key = makeSidecarKey(intent_admission_prefix, txn_id);
            var completion_writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
            defer completion_writes.deinit(self.alloc);
            try completion_writes.append(self.alloc, .{ .key = &rec_key, .value = marker_value });
            try completion_writes.appendSlice(self.alloc, extra_batch.completion_writes);
            if (resolved_participant_value) |value| {
                try completion_writes.append(self.alloc, .{ .key = &resolved_participant_key, .value = value });
            }
            try completion_writes.appendSlice(self.alloc, indexed_writes.items);
            var completion_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer completion_deletes.deinit(self.alloc);
            try completion_deletes.append(self.alloc, &stale_manifest_key);
            try completion_deletes.append(self.alloc, &stale_admission_key);
            try completion_deletes.append(self.alloc, &schema_lease_key);
            try completion_deletes.append(self.alloc, &prepared_key);
            try completion_deletes.append(self.alloc, &requirement_key);
            try completion_deletes.appendSlice(self.alloc, extra_batch.completion_deletes);
            try completion_deletes.appendSlice(self.alloc, indexed_deletes.items);
            const admission = try self.loadIntentAdmission(self.alloc, txn_id);
            try self.applyResolutionResourceBatch(completion_writes.items, completion_deletes.items, null, .{ .previous = if (admission) |value| value.reservation() else 0, .next = 0 }, null, control, if (status == .aborted) .{ .id = txn_id, .status = status } else null);
            return .{
                .applied = false,
                .replay_sequence = record.replay_sequence,
            };
        }

        if (extra_batch.skip_all_intent_application and extra_batch.skip_intent_keys.len != 0) {
            return error.InvalidArgument;
        }
        var skipped_intent_keys = std.StringHashMapUnmanaged(void).empty;
        defer skipped_intent_keys.deinit(self.alloc);
        if (extra_batch.skip_intent_keys.len != 0) {
            try skipped_intent_keys.ensureTotalCapacity(self.alloc, @intCast(extra_batch.skip_intent_keys.len));
            for (extra_batch.skip_intent_keys) |key| skipped_intent_keys.putAssumeCapacity(key, {});
        }

        var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
        defer writes.deinit(self.alloc);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);
        var owned_apply_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_apply_keys.items) |key| self.alloc.free(key);
            owned_apply_keys.deinit(self.alloc);
        }

        var read_count_value: [8]u8 = undefined;
        if (read_entries.len != 0) {
            const remaining = std.math.sub(u64, try self.readGuardCount(), read_entries.len) catch return error.InvalidTxnRecord;
            for (read_entries) |entry| {
                if (!validReadMember(entry.value)) return error.InvalidTxnRecord;
                const user_key = entry.key[read_prefix.len..];
                const guard_key = try makeReadGuardKey(self.alloc, user_key, txn_id);
                try appendOwnedBytes(self.alloc, &owned_apply_keys, guard_key);
                try deletes.append(self.alloc, guard_key);
                try deletes.append(self.alloc, entry.key);
            }
            if (remaining == 0) {
                try deletes.append(self.alloc, read_guard_count_key);
            } else {
                std.mem.writeInt(u64, &read_count_value, remaining, .little);
                try writes.append(self.alloc, .{ .key = read_guard_count_key, .value = &read_count_value });
            }
        }
        const read_admission_key = makeSidecarKey(read_admission_prefix, txn_id);
        try deletes.append(self.alloc, &read_admission_key);

        // Always delete the intent keys
        for (intent_entries) |entry| {
            try deletes.append(self.alloc, entry.key);
            const user_key = entry.key[intents_prefix.len + 17 ..];
            const member_key = try makeIntentMemberKey(self.alloc, txn_id, user_key);
            try appendOwnedBytes(self.alloc, &owned_apply_keys, member_key);
            try deletes.append(self.alloc, member_key);
            const lock_key = try self.makeIntentLockKey(user_key);
            try appendOwnedBytes(self.alloc, &owned_apply_keys, lock_key);
            try deletes.append(self.alloc, lock_key);
        }
        const intent_keys_key = makeSidecarKey(intent_keys_prefix, txn_id);
        const admission_key = makeSidecarKey(intent_admission_prefix, txn_id);
        try deletes.append(self.alloc, &admission_key);
        try deletes.append(self.alloc, &intent_keys_key);
        try deletes.append(self.alloc, &schema_lease_key);
        try deletes.append(self.alloc, &prepared_key);
        try deletes.append(self.alloc, &requirement_key);
        const publication_credit_key = publication_backlog.transactionCreditKey(txn_id);
        const publication_batch_key = makeTransactionReplicationBatchOutboxKey(txn_id);
        // Outbox creation and intents_resolved commit atomically. An already
        // resolved retry returned above and keeps its grant for HA delivery;
        // this transition only needs to inspect its own new writes.
        var keeps_publication_credit = false;
        if (status == .committed) for (extra_batch.writes) |write| {
            if (std.mem.eql(u8, write.key, &publication_batch_key)) keeps_publication_credit = true;
        };
        if (!keeps_publication_credit) try deletes.append(self.alloc, &publication_credit_key);
        if (status == .committed) {
            // Apply intents to real keys
            for (intent_entries) |entry| {
                // Extract user key from intent key:
                // intents_prefix(20) + txn_id(16) + ':'(1) + user_key
                const user_key = entry.key[intents_prefix.len + 17 ..];
                if (extra_batch.skip_all_intent_application or skipped_intent_keys.contains(user_key)) continue;

                const intent = try IntentValue.decode(entry.value);
                if (intent.prepared_row != null) return error.PreparedIntentRequiresMaterialization;
                // Integrity participants store private physical records, not
                // primary JSON rows. Recovery without a DB materialization
                // hook must preserve exactly the same physical namespace.
                if (isRawMetadataIntentKey(user_key)) {
                    if (intent.value) |value| {
                        try writes.append(self.alloc, .{ .key = user_key, .value = value });
                    } else try deletes.append(self.alloc, user_key);
                    continue;
                }
                if (intent.value == null) {
                    // Delete — also remove the timestamp entry
                    const store_key = try internal_keys.documentKeyAlloc(self.alloc, user_key);
                    try appendOwnedBytes(self.alloc, &owned_apply_keys, store_key);
                    try deletes.append(self.alloc, store_key);

                    const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, user_key);
                    try appendOwnedBytes(self.alloc, &owned_apply_keys, ts_key);
                    try deletes.append(self.alloc, ts_key);
                } else {
                    // Put
                    const val = intent.value.?;
                    const store_key = try internal_keys.documentKeyAlloc(self.alloc, user_key);
                    try appendOwnedBytes(self.alloc, &owned_apply_keys, store_key);
                    try writes.append(self.alloc, .{ .key = store_key, .value = val });

                    // Write timestamp for the key
                    const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, user_key);
                    try appendOwnedBytes(self.alloc, &owned_apply_keys, ts_key);
                    var ts_val: [8]u8 = undefined;
                    std.mem.writeInt(u64, &ts_val, timestamp, .little);
                    try writes.append(self.alloc, .{ .key = ts_key, .value = &ts_val });
                }
            }
        }

        if (status == .committed) {
            if (extra_batch.replay) |replay| record.replay_sequence = replay.sequence;
        }
        record.intents_resolved = true;
        record.intents_resolved_known = true;
        const rec_val = try self.encodeRecord(record);
        defer self.alloc.free(rec_val);
        try writes.append(self.alloc, .{ .key = &rec_key, .value = rec_val });
        try writes.appendSlice(self.alloc, extra_batch.writes);
        try writes.appendSlice(self.alloc, extra_batch.completion_writes);
        if (resolved_participant_value) |value| {
            try writes.append(self.alloc, .{ .key = &resolved_participant_key, .value = value });
        }
        try writes.appendSlice(self.alloc, indexed_writes.items);
        try deletes.appendSlice(self.alloc, extra_batch.deletes);
        try deletes.appendSlice(self.alloc, extra_batch.completion_deletes);
        try deletes.appendSlice(self.alloc, indexed_deletes.items);

        const admission = try self.loadIntentAdmission(self.alloc, txn_id);
        try self.applyResolutionResourceBatch(writes.items, deletes.items, extra_batch.replay, .{ .previous = if (admission) |value| value.reservation() else 0, .next = 0 }, if (status == .committed) extra_batch.commit_participant else null, control, .{ .id = txn_id, .status = status });

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

    /// Metadata probes intentionally have no iterator. Use the backend's
    /// short current-scan lease for this bounded, keys-only presence check.
    pub fn hasPreparedCatalogLease(self: *TxnManager) !bool {
        var scan = try self.store.beginCurrentScan();
        defer scan.abort();
        return try @import("db/artifact_inventory.zig").hasPreparedCatalogLease(&scan);
    }

    /// Activation must not retroactively authorize unfenced required records,
    /// including opaque/orphan metadata. Bounded seeks never decode payloads.
    pub fn hasPreparationMetadata(self: *TxnManager) !bool {
        var scan = try self.store.beginCurrentScan();
        defer scan.abort();
        var cursor = try scan.openCursor();
        defer cursor.close();
        cursor.setKeysOnly();
        for ([_][]const u8{ publication_backlog.prepared_effects_prefix, publication_backlog.preparation_requirement_prefix }) |prefix| {
            if (try cursor.seekAtOrAfter(prefix)) |entry|
                if (std.mem.startsWith(u8, entry.key, prefix)) return true;
        }
        return false;
    }

    pub fn hasPreparedEffects(self: *TxnManager, txn_id: TxnId) !bool {
        const key = makePreparedEffectsKey(txn_id);
        return self.keyExists(&key);
    }

    fn hashPreparedInput(hash: *std.crypto.hash.Blake3, bytes: []const u8, control: ?read_control.Port) !void {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, bytes.len, .little);
        hash.update(&length);
        var offset: usize = 0;
        while (offset < bytes.len) {
            if (control) |owner| try owner.checkpoint();
            const end = offset + @min(@as(usize, 4096), bytes.len - offset);
            hash.update(bytes[offset..end]);
            offset = end;
        }
    }

    /// Capture outside producer execution, then revalidate at seal under the
    /// same external DB apply fence used by every TxnManager mutation. Digests
    /// of catalogs are authored/leased by the DB catalog owner, not this layer.
    pub fn preparedEffectsContract(self: *TxnManager, txn_id: TxnId, catalog_digest: [32]u8, mode: prepared_effects.Mode, original: ?read_control.Port) !prepared_effects.Contract {
        var trace: MutationAdmissionTrace = .{};
        return self.preparedEffectsContractInner(txn_id, catalog_digest, mode, trace.bind(original)) catch |err| return trace.failure orelse err;
    }

    fn preparedEffectsContractInner(self: *TxnManager, txn_id: TxnId, catalog_digest: [32]u8, mode: prepared_effects.Mode, control: ?read_control.Port) !prepared_effects.Contract {
        var snapshot = try self.capturePreparedIntentsInner(self.alloc, txn_id, catalog_digest, mode, control);
        defer snapshot.deinit();
        return snapshot.contract;
    }

    /// The returned batch and contract describe one captured revision, including
    /// exact stored prepared-row envelopes. This does not certify a subsequently
    /// supplied DerivedBatch or reserve the executable completion bundle. Only
    /// pending transactions may capture producer inputs; recovery loads already
    /// accepted effects through its separate contract validation path.
    pub fn capturePreparedIntents(self: *TxnManager, alloc: Allocator, txn_id: TxnId, catalog_digest: [32]u8, mode: prepared_effects.Mode, original: ?read_control.Port) !PreparedIntentSnapshot {
        var trace: MutationAdmissionTrace = .{};
        var snapshot = self.captureProducerInputsInner(alloc, txn_id, catalog_digest, mode, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| {
            snapshot.deinit();
            return cause;
        }
        return snapshot;
    }

    fn captureProducerInputsInner(self: *TxnManager, alloc: Allocator, txn_id: TxnId, catalog_digest: [32]u8, mode: prepared_effects.Mode, control: ?read_control.Port) !PreparedIntentSnapshot {
        if (control) |owner| try owner.checkpoint();
        const decision = try self.getPreparationDecision(txn_id, control);
        if (decision.status != .pending) return error.DecisionConflict;
        if (decision.requirement) |required| if (required.mode != mode) return error.PreparationContractConflict;
        {
            // A retry consumes the accepted frame. Key-only lookup must reject
            // even corrupt/opaque seals without loading their effect payloads.
            const key = makePreparedEffectsKey(txn_id);
            var scan = if (control) |owner| try self.store.beginCurrentScanWithAdmission(owner) else try self.store.beginCurrentScan();
            defer scan.abort();
            var cursor = try scan.openCursor();
            defer cursor.close();
            cursor.setKeysOnly();
            if (try cursor.seekAtOrAfter(&key)) |entry|
                if (std.mem.eql(u8, entry.key, &key)) return error.PreparedEffectsSealed;
        }
        return self.capturePreparedIntentsInner(alloc, txn_id, catalog_digest, mode, control);
    }

    fn capturePreparedIntentsInner(self: *TxnManager, alloc: Allocator, txn_id: TxnId, catalog_digest: [32]u8, mode: prepared_effects.Mode, control: ?read_control.Port) !PreparedIntentSnapshot {
        if (control) |owner| try owner.checkpoint();
        const record = try self.loadTransactionRecordWithAdmission(txn_id, control);
        if (!record.prepared or record.intent_revision == 0 or record.status == .aborted) return error.PreparedEffectsRequired;
        var batch = try self.collectIntentBatchWithAdmission(alloc, txn_id, control);
        errdefer batch.deinit(alloc);
        if (batch.revision != record.intent_revision) return error.IntentSnapshotChanged;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-prepared-intents-v1");
        hash.update(&txn_id);
        var identity: [24]u8 = undefined;
        std.mem.writeInt(u64, identity[0..8], record.begin_timestamp, .little);
        std.mem.writeInt(u64, identity[8..16], record.created_at, .little);
        std.mem.writeInt(u64, identity[16..24], batch.revision, .little);
        hash.update(&identity);
        // Collection returns physical keys in sorted order, for both legacy
        // manifests and the current member index. Length framing distinguishes
        // boundaries, deletes and prepared-row envelopes without re-parsing JSON.
        for (batch.owned_entries.?) |entry| {
            if (control) |owner| try owner.checkpoint();
            try hashPreparedInput(&hash, entry.key, control);
            try hashPreparedInput(&hash, entry.value, control);
        }
        const after = try self.loadTransactionRecordWithAdmission(txn_id, control);
        if (after.intent_revision != batch.revision or after.status != record.status or
            after.begin_timestamp != record.begin_timestamp or after.created_at != record.created_at or
            !after.prepared) return error.IntentSnapshotChanged;
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return .{ .allocator = alloc, .batch = batch, .contract = .{
            .txn_id = txn_id,
            .intent_revision = batch.revision,
            .intent_digest = digest,
            .catalog_digest = catalog_digest,
            .schema_version = if (batch.schema_binding) |binding| binding.version else null,
            .mode = mode,
        } };
    }

    /// Final predecision seal. The external apply fence must span validation
    /// and publication. Normal HTTP/SQL prepare does not call this API yet.
    /// Its immutable frame shares durable backlog admission and the backend
    /// transaction with its size/aggregate accounting. Failed admission leaves
    /// the existing revision mutable; accepted seals freeze subsequent writes.
    pub fn sealPreparedEffects(self: *TxnManager, txn_id: TxnId, frame: []const u8, catalog_digest: [32]u8, mode: prepared_effects.Mode, original: ?read_control.Port) !void {
        return self.sealPreparedEffectsWithExtraBatch(txn_id, frame, catalog_digest, mode, .{}, original);
    }

    /// Publish an ordered receipt in the same transaction as the immutable
    /// frame and its backlog grant. A retry preserves the first accepted frame.
    pub fn sealPreparedEffectsWithExtraBatch(self: *TxnManager, txn_id: TxnId, frame: []const u8, catalog_digest: [32]u8, mode: prepared_effects.Mode, extra_batch: MutationExtraBatch, original: ?read_control.Port) !void {
        return self.sealBoundPreparedEffectsWithExtraBatch(txn_id, frame, catalog_digest, mode, null, extra_batch, original);
    }

    pub fn sealBoundPreparedEffectsWithExtraBatch(self: *TxnManager, txn_id: TxnId, frame: []const u8, catalog_digest: [32]u8, mode: prepared_effects.Mode, binding: ?prepared_effects.Binding, extra_batch: MutationExtraBatch, original: ?read_control.Port) !void {
        var trace: MutationAdmissionTrace = .{};
        self.sealPreparedEffectsInner(txn_id, frame, catalog_digest, mode, binding, extra_batch, null, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
    }

    /// Trusted canonical request only: the caller must bind all request and
    /// auxiliary effects to the authoritative input/catalog/route witness. This
    /// funds HA replay retention, not derived-log or executable recovery work.
    pub fn sealPreparedEffectsAndReplayPlan(self: *TxnManager, txn_id: TxnId, frame: []const u8, catalog_digest: [32]u8, mode: prepared_effects.Mode, binding: ?prepared_effects.Binding, request: @import("db/derived/derived_types.zig").DerivedBatch, extra_batch: MutationExtraBatch, original: ?read_control.Port) !void {
        var trace: MutationAdmissionTrace = .{};
        self.sealPreparedEffectsInner(txn_id, frame, catalog_digest, mode, binding, extra_batch, request, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
    }

    fn sealPreparedEffectsInner(self: *TxnManager, txn_id: TxnId, frame: []const u8, catalog_digest: [32]u8, mode: prepared_effects.Mode, binding: ?prepared_effects.Binding, extra_batch: MutationExtraBatch, request: ?@import("db/derived/derived_types.zig").DerivedBatch, control: ?read_control.Port) !void {
        const key = makePreparedEffectsKey(txn_id);
        const requirement_key = makePreparationRequirementKey(txn_id);
        const record_key = makeRecordKey(txn_id);
        for (extra_batch.writes) |write| {
            if (control) |owner| try owner.checkpoint();
            if (std.mem.eql(u8, write.key, &key) or std.mem.eql(u8, write.key, &requirement_key) or std.mem.eql(u8, write.key, &record_key)) return error.InvalidPreparationRequirement;
        }
        for (extra_batch.deletes) |deleted| {
            if (control) |owner| try owner.checkpoint();
            if (std.mem.eql(u8, deleted, &key) or std.mem.eql(u8, deleted, &requirement_key) or std.mem.eql(u8, deleted, &record_key)) return error.InvalidPreparationRequirement;
        }
        const record = try self.loadTransactionRecordWithAdmission(txn_id, control);
        if (record.status != .pending) return TxnError.DecisionConflict;
        if (!std.meta.eql(record.preparation_binding, binding)) return error.PreparationContractConflict;
        if (try self.loadPreparationRequirementInner(txn_id, control)) |required| if (required.mode != mode) return error.PreparationContractConflict;
        const inspection = try prepared_effects.inspect(frame, control);
        var replay_plan: ?@import("prepared_replay_plan.zig").Plan = null;
        defer if (replay_plan) |*plan| plan.deinit();
        {
            // Hash and plan from the same owned inputs. Release the snapshot and
            // decode slab before opening the publication writer; only roots and
            // scalar wire totals survive into the atomic seal.
            var inputs = try self.capturePreparedIntentsInner(self.alloc, txn_id, catalog_digest, mode, control);
            defer inputs.deinit();
            const expected = inputs.contract;
            if (!std.meta.eql(expected, inspection.contract)) return error.PreparedEffectsMismatch;
            if (request) |canonical| {
                var writes: std.ArrayListUnmanaged([]const u8) = .empty;
                defer writes.deinit(self.alloc);
                var deletes: std.ArrayListUnmanaged([]const u8) = .empty;
                defer deletes.deinit(self.alloc);
                for (inputs.batch.writes) |write| {
                    if (control) |owner| try owner.checkpoint();
                    if (!isRawMetadataIntentKey(write.key)) try writes.append(self.alloc, write.key);
                }
                for (inputs.batch.deletes) |deleted| {
                    if (control) |owner| try owner.checkpoint();
                    if (!isRawMetadataIntentKey(deleted)) try deletes.append(self.alloc, deleted);
                }
                var decoded = try prepared_effects.decode(self.alloc, frame, expected, control);
                defer decoded.deinit();
                replay_plan = try @import("prepared_replay_plan.zig").deriveWithPrimaryKeys(self.alloc, .{ .writes = writes.items, .deletes = deletes.items }, canonical, decoded.plan.effects, control);
            }
        }
        const reservation: ?backend_erased.PreparedReplayReservation = if (replay_plan) |plan| .{
            .txn_id = txn_id,
            .frame = inspection.fingerprint,
            .base = plan.base,
            .roots = plan.roots,
        } else null;
        const previous = self.getAllocWithAdmission(self.alloc, &key, control) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        defer if (previous) |bytes| self.alloc.free(bytes);
        if (previous) |bytes| {
            // Lost response retry: preserve the first accepted immutable result,
            // revision and grant. A regenerated/different result cannot replace it.
            if (bytes.len != frame.len) return error.PreparedEffectsConflict;
            var offset: usize = 0;
            while (offset < bytes.len) {
                if (control) |owner| try owner.checkpoint();
                const end = offset + @min(@as(usize, 4096), bytes.len - offset);
                if (!std.mem.eql(u8, bytes[offset..end], frame[offset..end])) return error.PreparedEffectsConflict;
                offset = end;
            }
            try self.applyPreparedSealBatch(extra_batch.writes, extra_batch.deletes, reservation, control);
            return;
        }
        var writes: std.ArrayListUnmanaged(docstore.KVPair) = .empty;
        defer writes.deinit(self.alloc);
        try writes.append(self.alloc, .{ .key = &key, .value = frame });
        try writes.appendSlice(self.alloc, extra_batch.writes);
        try self.applyPreparedSealBatch(writes.items, extra_batch.deletes, reservation, control);
    }

    /// Reopen/recovery owns both this decoded slab and any separately staged
    /// materialization scratch. This load does not invoke a producer or infer
    /// its mode from the retry's acknowledgement level.
    pub fn loadPreparedEffects(self: *TxnManager, alloc: Allocator, txn_id: TxnId, original: ?read_control.Port) !?PreparedEffects {
        var captured = (try self.loadPreparedEffectsWithInputs(alloc, txn_id, original)) orelse return null;
        defer captured.inputs.deinit();
        return captured.effects;
    }

    /// Caller holds the external apply fence while capturing. Both owners
    /// survive that short fence and feed the canonical request construction;
    /// final publication still revalidates the revision/catalog contract.
    pub fn loadPreparedEffectsWithInputs(self: *TxnManager, alloc: Allocator, txn_id: TxnId, original: ?read_control.Port) !?PreparedResolutionInputs {
        var trace: MutationAdmissionTrace = .{};
        const result = self.loadPreparedEffectsInner(alloc, txn_id, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| {
            if (result) |value| {
                var owned = value;
                owned.deinit();
            }
            return cause;
        }
        return result;
    }

    fn loadPreparedEffectsInner(self: *TxnManager, alloc: Allocator, txn_id: TxnId, control: ?read_control.Port) !?PreparedResolutionInputs {
        const key = makePreparedEffectsKey(txn_id);
        const raw = self.getAllocWithAdmission(alloc, &key, control) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(raw);
        const inspection = try prepared_effects.inspect(raw, control);
        if (try self.loadPreparationRequirementInner(txn_id, control)) |required| if (required.mode != inspection.contract.mode) return error.PreparationContractConflict;
        var inputs = try self.capturePreparedIntentsInner(alloc, txn_id, inspection.contract.catalog_digest, inspection.contract.mode, control);
        errdefer inputs.deinit();
        return .{ .effects = .{ .decoded = try prepared_effects.decode(alloc, raw, inputs.contract, control), .fingerprint = inspection.fingerprint }, .inputs = inputs };
    }

    pub fn collectIntentBatch(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !IntentBatch {
        return self.collectIntentBatchWithAdmission(alloc, txn_id, null);
    }

    pub fn collectIntentBatchWithAdmission(self: *TxnManager, alloc: Allocator, txn_id: TxnId, control: ?read_control.Port) !IntentBatch {
        if (control) |owner| try owner.checkpoint();
        // Read the revision before scanning. Intent writes publish the record
        // revision and intent rows in one backend batch, so validation under
        // the DB apply lock detects any prepare that raced with this snapshot.
        const record = try self.loadTransactionRecord(txn_id);
        const schema_binding = try self.loadSchemaBinding(alloc, txn_id);
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        const intent_entries = try self.loadIntentEntriesWithAdmission(alloc, txn_id, scan_prefix, control);
        errdefer backend_scan.freeResults(alloc, intent_entries);

        var write_count: usize = 0;
        for (intent_entries) |entry| {
            if (control) |owner| try owner.checkpoint();
            if ((try IntentValue.decode(entry.value)).value != null) write_count += 1;
        }
        const writes = try alloc.alloc(docstore.KVPair, write_count);
        errdefer alloc.free(writes);
        const prepared_rows = try alloc.alloc(?[]const u8, write_count);
        errdefer alloc.free(prepared_rows);
        const deletes = try alloc.alloc([]const u8, intent_entries.len - write_count);
        errdefer alloc.free(deletes);
        var writes_initialized: usize = 0;
        var deletes_initialized: usize = 0;

        for (intent_entries) |entry| {
            if (control) |owner| try owner.checkpoint();
            const user_key = entry.key[intents_prefix.len + 17 ..];
            const intent = try IntentValue.decode(entry.value);
            if (intent.value == null) {
                deletes[deletes_initialized] = user_key;
                deletes_initialized += 1;
            } else {
                writes[writes_initialized] = .{ .key = user_key, .value = intent.value.? };
                prepared_rows[writes_initialized] = intent.prepared_row;
                writes_initialized += 1;
            }
        }
        if (control) |owner| try owner.checkpoint();
        return .{ .writes = writes, .deletes = deletes, .prepared_rows = prepared_rows, .revision = record.intent_revision, .schema_binding = schema_binding, .owned_entries = intent_entries };
    }

    pub fn loadSchemaBinding(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !?SchemaBinding {
        const key = makeSidecarKey(schema_leases_prefix, txn_id);
        const raw = self.getAlloc(alloc, &key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(raw);
        if (raw.len != 5 or raw[0] > 1) return error.InvalidTxnRecord;
        if (raw[0] == 0 and std.mem.readInt(u32, raw[1..5], .little) != 0) return error.InvalidTxnRecord;
        return .{ .version = if (raw[0] == 1) std.mem.readInt(u32, raw[1..5], .little) else null };
    }

    /// Called under the same apply fence as schema publication. A current
    /// cursor holds a short read lease, without cloning the mutable memtable.
    pub fn hasSchemaLeases(self: *TxnManager) !bool {
        var scan = try self.store.beginCurrentScan();
        defer scan.abort();
        var cursor = try scan.openCursor();
        defer cursor.close();
        cursor.setKeysOnly();
        // The frame also pins physical ownership/schema if an orphan or old
        // compatibility record lost its ordinary schema sidecar.
        for ([_][]const u8{ schema_leases_prefix, publication_backlog.prepared_effects_prefix }) |prefix| {
            if (try cursor.seekAtOrAfter(prefix)) |entry|
                if (std.mem.startsWith(u8, entry.key, prefix)) return true;
        }
        // Existing document databases may have unresolved intents predating
        // relational epoch binding. They also fence a storage-mode switch.
        const entry = (try cursor.seekAtOrAfter(intents_prefix)) orelse return false;
        return std.mem.startsWith(u8, entry.key, intents_prefix);
    }

    pub fn hasIntents(self: *TxnManager, txn_id: TxnId) !bool {
        const record = try self.loadTransactionRecord(txn_id);
        if (record.intents_resolved_known and record.intents_resolved) return false;
        if ((try self.loadReadAdmission(txn_id)).count != 0) return true;
        if (try self.loadIntentAdmission(self.alloc, txn_id)) |admission| return admission.count != 0;
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        if (try self.loadIntentKeysOptional(self.alloc, txn_id)) |intent_keys| {
            defer freeParticipantList(self.alloc, intent_keys);
            return intent_keys.len > 0;
        }
        // Compatibility for a pending transaction written before manifests.
        const entries = try backend_scan.scanPrefix(self.alloc, &self.store, prefix);
        defer backend_scan.freeResults(self.alloc, entries);
        return entries.len > 0;
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

    pub fn loadReplicationOutbox(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !ReplicationOutbox {
        const batch_key = makeTransactionReplicationBatchOutboxKey(txn_id);
        const replay_key = makeTransactionReplicationReplayOutboxKey(txn_id);
        var out: ReplicationOutbox = .{};
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

    pub fn hasReplicationOutbox(self: *TxnManager, txn_id: TxnId) !bool {
        const batch_key = makeTransactionReplicationBatchOutboxKey(txn_id);
        const replay_key = makeTransactionReplicationReplayOutboxKey(txn_id);
        return try self.keyExists(&batch_key) or try self.keyExists(&replay_key);
    }

    pub fn clearReplicationOutbox(self: *TxnManager, txn_id: TxnId, kind: ReplicationOutboxKind) !void {
        switch (kind) {
            .batch => {
                const key = makeTransactionReplicationBatchOutboxKey(txn_id);
                const credit_key = publication_backlog.transactionCreditKey(txn_id);
                try self.applyBatch(&.{}, &.{ &key, &credit_key }, null);
            },
            .replay => {
                const key = makeTransactionReplicationReplayOutboxKey(txn_id);
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

        const intent_entries = try self.loadIntentEntries(alloc, txn_id, scan_prefix);
        defer backend_scan.freeResults(alloc, intent_entries);

        for (intent_entries) |entry| {
            const user_key = entry.key[intents_prefix.len + 17 ..];
            const owned_key = try alloc.dupe(u8, user_key);
            if ((try IntentValue.decode(entry.value)).value == null) {
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

        const intent_entries = try self.loadIntentEntries(alloc, txn_id, scan_prefix);
        defer backend_scan.freeResults(alloc, intent_entries);

        var out = std.ArrayListUnmanaged(OwnedIntentMutation).empty;
        errdefer {
            for (out.items) |*item| item.deinit(alloc);
            out.deinit(alloc);
        }
        for (intent_entries) |entry| {
            const user_key = entry.key[intents_prefix.len + 17 ..];
            var owned_key: ?[]u8 = try alloc.dupe(u8, user_key);
            errdefer if (owned_key) |key| alloc.free(key);
            const intent = try IntentValue.decode(entry.value);
            var owned_value: ?[]u8 = if (intent.value) |value| try alloc.dupe(u8, value) else null;
            errdefer if (owned_value) |value| alloc.free(value);
            // A typed intent's value is a special-field sidecar, not a logical
            // document. Callers must use the schema-aware IntentBatch path.
            if (intent.prepared_row != null) return error.PreparedIntentRequiresMaterialization;
            try out.append(alloc, .{ .key = owned_key.?, .value = owned_value });
            owned_key = null;
            owned_value = null;
        }
        return try out.toOwnedSlice(alloc);
    }

    /// Get the status of a transaction.
    pub fn getTransactionStatus(self: *TxnManager, txn_id: TxnId) !TxnStatus {
        return (try self.loadTransactionRecord(txn_id)).status;
    }

    /// Preserve the original admission error even when it shares the store's
    /// missing-record name and loadTransactionRecord translates that name.
    pub fn getTransactionStatusWithAdmission(self: *TxnManager, txn_id: TxnId, original: ?read_control.Port) !TxnStatus {
        var trace: MutationAdmissionTrace = .{};
        const record = self.loadTransactionRecordWithAdmission(txn_id, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
        return record.status;
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

    pub fn retainsCoordinatorAcknowledgement(self: *TxnManager, txn_id: TxnId) !bool {
        const record = try self.loadTransactionRecord(txn_id);
        return record.coordinator and record.retain_terminal;
    }

    pub fn listTransactions(self: *TxnManager, alloc: Allocator) ![]TxnSummary {
        return (try self.listTransactionsPage(alloc, null, std.math.maxInt(usize))).items;
    }

    /// Index READY publication needs to drain outstanding row intents, not
    /// idle/read-only sessions or already resolved HA outboxes. Admission and
    /// publication are serialized by the DB apply fence.
    pub fn hasPendingIntents(self: *TxnManager) !bool {
        var read = try self.store.beginRead();
        defer read.abort();
        return readHasPrefix(&read, intents_prefix);
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
            try readHasPrefix(&read, read_guards_prefix) or
            try readHasPrefix(&read, read_members_prefix) or
            try readHasPrefix(&read, hot_standby_batch_outbox_prefix) or
            try readHasPrefix(&read, hot_standby_replay_outbox_prefix))
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
                const index_key = makeSidecarKey(participant_index_prefix, txn_id);
                if (read.get(&index_key)) |raw_index| {
                    const indexed = try ParticipantIndex.decode(raw_index);
                    const legacy = read.get(&resolved_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (legacy != null) {
                        const members = try resolvedParticipantsInRead(self.alloc, &read, txn_id);
                        defer freeParticipantList(self.alloc, members);
                        if (members.len > indexed.enlisted) return error.InvalidTxnRecord;
                        if (members.len != indexed.enlisted) return true;
                    } else if (indexed.resolved != indexed.enlisted) return true;
                    entry = try cursor.next();
                    continue;
                } else |err| if (err != error.NotFound) return err;
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

    /// Constant-work uncertainty check for coordinated statement snapshots.
    /// The caller must hold primary AND replay mutation admission while using
    /// this result. A prepared participant may otherwise resolve immediately
    /// after the check, yielding different sides of one distributed commit.
    pub fn hasUnresolvedWriteIntents(self: *TxnManager) !bool {
        return self.hasUnresolvedWriteIntentsWithAdmission(null);
    }
    pub fn hasUnresolvedWriteIntentsWithAdmission(self: *TxnManager, original: ?@import("../common/read_admission.zig").Port) !bool {
        var retained: @import("../common/read_admission.zig").RetainedDependency = .{};
        if (retained.bind(original)) |port| {
            try port.checkpoint();
            // Both primary and replay fences remain owned. Busy providers
            // unwind to the coordinator; they cannot lend CPU to a producer
            // that may itself need either fence.
            var read = try self.store.beginReadWithAdmission(.transient, port);
            defer read.abort();
            var cursor = try read.openCursorWithAdmission(port);
            defer cursor.close();
            const entry = try cursor.seekAtOrAfterWithAdmission(intents_prefix, port);
            try port.checkpoint();
            return if (entry) |value| std.mem.startsWith(u8, value.key, intents_prefix) else false;
        }
        var read = try self.store.beginRead();
        defer read.abort();
        return readHasPrefix(&read, intents_prefix);
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
                .intents_resolved = record.intents_resolved,
                .intents_resolved_known = record.intents_resolved_known,
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
        try self.markParticipantResolvedExtraBatch(txn_id, participant, .{});
    }

    pub fn markParticipantResolvedExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        participant: []const u8,
        extra_batch: MutationExtraBatch,
    ) !void {
        const index_key = makeSidecarKey(participant_index_prefix, txn_id);
        if (try self.keyExists(&index_key)) return self.markParticipantsResolvedExtraBatch(txn_id, &.{participant}, extra_batch);
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
            if (std.mem.eql(u8, existing, participant)) {
                try self.applyMutationExtraBatch(extra_batch);
                return;
            }
        }

        var next = try self.alloc.alloc([]u8, resolved.len + 1);
        var initialized: usize = 0;
        defer {
            for (next[0..initialized]) |entry| self.alloc.free(entry);
            self.alloc.free(next);
        }
        for (resolved, 0..) |existing, i| {
            next[i] = try self.alloc.dupe(u8, existing);
            initialized += 1;
        }
        next[resolved.len] = try self.alloc.dupe(u8, participant);
        initialized += 1;
        const key = makeSidecarKey(resolved_participants_prefix, txn_id);
        const encoded = try encodeParticipantList(self.alloc, next);
        defer self.alloc.free(encoded);
        var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
        defer writes.deinit(self.alloc);
        try writes.append(self.alloc, .{ .key = &key, .value = encoded });
        try writes.appendSlice(self.alloc, extra_batch.writes);
        try self.applyBatch(writes.items, extra_batch.deletes, null);
    }

    pub fn markParticipantsResolvedExtraBatch(self: *TxnManager, txn_id: TxnId, acknowledgements: []const []const u8, extra_batch: MutationExtraBatch) !void {
        return self.markParticipantsResolvedExtraBatchWithAdmission(txn_id, acknowledgements, extra_batch, null);
    }

    pub fn markParticipantsResolvedExtraBatchWithAdmission(self: *TxnManager, txn_id: TxnId, acknowledgements: []const []const u8, extra_batch: MutationExtraBatch, original: ?read_control.Port) !void {
        var trace: read_control.RejectionTrace = .{};
        const control = trace.bind(original);
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var writes: std.ArrayListUnmanaged(docstore.KVPair) = .empty;
        var deletes: std.ArrayListUnmanaged([]const u8) = .empty;
        self.stageParticipantResolutionsWithAdmission(txn_id, acknowledgements, scratch, &writes, &deletes, control) catch |err| return trace.failure orelse err;
        try writes.appendSlice(scratch, extra_batch.writes);
        try deletes.appendSlice(scratch, extra_batch.deletes);
        self.applyBatchWithAdmission(writes.items, deletes.items, null, control) catch |err| return trace.failure orelse err;
    }

    /// Stage indexed acknowledgements into the caller's atomic mutation batch.
    /// Every key/value added here lives in scratch until that batch finishes.
    fn stageParticipantResolutions(self: *TxnManager, txn_id: TxnId, acknowledgements: []const []const u8, scratch: Allocator, writes: *std.ArrayListUnmanaged(docstore.KVPair), deletes: *std.ArrayListUnmanaged([]const u8)) !void {
        return self.stageParticipantResolutionsWithAdmission(txn_id, acknowledgements, scratch, writes, deletes, null);
    }

    fn stageParticipantResolutionsWithAdmission(self: *TxnManager, txn_id: TxnId, acknowledgements: []const []const u8, scratch: Allocator, writes: *std.ArrayListUnmanaged(docstore.KVPair), deletes: *std.ArrayListUnmanaged([]const u8), control: ?read_control.Port) !void {
        if (control) |owner| try owner.checkpoint();
        if (acknowledgements.len == 0 or acknowledgements.len > 64) return error.InvalidParticipant;
        _ = try self.loadTransactionRecordWithAdmission(txn_id, control);
        const index_key = makeSidecarKey(participant_index_prefix, txn_id);
        const raw = self.getAllocWithAdmission(scratch, &index_key, control) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var indexed = if (raw) |bytes| try ParticipantIndex.decode(bytes) else ParticipantIndex{};
        var enlisted: std.StringHashMapUnmanaged(void) = .empty;
        if (raw == null) {
            const participants = try self.loadParticipantSetWithAdmission(scratch, participants_prefix, txn_id, control);
            for (participants) |participant| {
                if (control) |owner| try owner.checkpoint();
                const member = try enlisted.getOrPut(scratch, participant);
                if (member.found_existing) continue;
                indexed.enlisted += 1;
                const key = try scratch.dupe(u8, &makeParticipantIndexKey(participant_index_prefix, txn_id, participant));
                try writes.append(scratch, .{ .key = key, .value = participant });
            }
        }
        // Validate every member before any mutation. The immutable membership
        // index replaces repeated cohort-sized decode and membership scans.
        for (acknowledgements) |participant| {
            if (control) |owner| try owner.checkpoint();
            if (raw == null) {
                if (!enlisted.contains(participant)) return error.InvalidParticipant;
            } else {
                const key = makeParticipantIndexKey(participant_index_prefix, txn_id, participant);
                const member = self.getAllocWithAdmission(scratch, &key, control) catch |err| switch (err) {
                    error.NotFound => return error.InvalidParticipant,
                    else => return err,
                };
                if (!std.mem.eql(u8, member, participant)) return error.InvalidTxnRecord;
            }
        }
        var resolved: std.StringHashMapUnmanaged(void) = .empty;
        // A local resolution may atomically have used the legacy sidecar.
        // Absorb it once, preserving all independently durable evidence.
        const legacy_key = makeSidecarKey(resolved_participants_prefix, txn_id);
        const legacy_members = try self.loadParticipantSetWithAdmission(scratch, resolved_participants_prefix, txn_id, control);
        for (legacy_members) |participant| {
            if (control) |owner| try owner.checkpoint();
            if (raw == null) {
                if (!enlisted.contains(participant)) return error.InvalidTxnRecord;
            } else {
                const member_key = makeParticipantIndexKey(participant_index_prefix, txn_id, participant);
                const member = self.getAllocWithAdmission(scratch, &member_key, control) catch |err| switch (err) {
                    error.NotFound => return error.InvalidTxnRecord,
                    else => return err,
                };
                if (!std.mem.eql(u8, member, participant)) return error.InvalidTxnRecord;
            }
            try resolved.put(scratch, participant, {});
        }
        for (acknowledgements) |participant| {
            if (control) |owner| try owner.checkpoint();
            try resolved.put(scratch, participant, {});
        }
        var members = resolved.keyIterator();
        while (members.next()) |participant| {
            if (control) |owner| try owner.checkpoint();
            const key = makeParticipantIndexKey(resolved_index_prefix, txn_id, participant.*);
            const existing = self.getAllocWithAdmission(scratch, &key, control) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (existing) |value| {
                if (!std.mem.eql(u8, value, participant.*)) return error.InvalidTxnRecord;
            } else {
                indexed.resolved += 1;
                try writes.append(scratch, .{ .key = try scratch.dupe(u8, &key), .value = participant.* });
            }
        }
        if (indexed.resolved > indexed.enlisted) return error.InvalidTxnRecord;
        var encoded: [17]u8 = undefined;
        indexed.encode(&encoded);
        try writes.append(scratch, .{ .key = try scratch.dupe(u8, &index_key), .value = try scratch.dupe(u8, &encoded) });
        try deletes.append(scratch, try scratch.dupe(u8, &legacy_key));
    }

    pub fn getParticipants(self: *TxnManager, alloc: Allocator, txn_id: TxnId) ![][]u8 {
        return try self.loadParticipantSet(alloc, participants_prefix, txn_id);
    }

    pub fn getResolvedParticipants(self: *TxnManager, alloc: Allocator, txn_id: TxnId) ![][]u8 {
        const index_key = makeSidecarKey(participant_index_prefix, txn_id);
        if (!(try self.keyExists(&index_key))) return self.loadParticipantSet(alloc, resolved_participants_prefix, txn_id);
        var read = try self.store.beginRead();
        defer read.abort();
        return resolvedParticipantsInRead(alloc, &read, txn_id);
    }

    pub fn getUnresolvedParticipants(self: *TxnManager, alloc: Allocator, txn_id: TxnId) ![][]u8 {
        const participants = try self.getParticipants(alloc, txn_id);
        defer freeParticipantList(alloc, participants);
        const resolved = try self.getResolvedParticipants(alloc, txn_id);
        defer freeParticipantList(alloc, resolved);

        var resolved_set: std.StringHashMapUnmanaged(void) = .empty;
        defer resolved_set.deinit(alloc);
        for (resolved) |participant| try resolved_set.put(alloc, participant, {});
        var unresolved = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (unresolved.items) |entry| alloc.free(entry);
            unresolved.deinit(alloc);
        }

        try unresolved.ensureTotalCapacity(alloc, participants.len);
        for (participants) |participant| {
            if (resolved_set.contains(participant)) {
                continue;
            }
            unresolved.appendAssumeCapacity(try alloc.dupe(u8, participant));
        }

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

            if (try self.hasPreparedEffects(txn_id) or try self.hasPreparationRequirement(txn_id)) {
                if (summary.status == .committed) {
                    // Empty row intents do not prove accepted effects were
                    // consumed. Generic legacy recovery must leave these to
                    // the plan-aware owner and continue draining other records.
                    stats.deferred_unresolved += 1;
                    continue;
                }
                // Authoritative abort can retire even an opaque/corrupt plan
                // or requirement. Preserve ordinary completion hooks, but do
                // not interpret stale summary flags again after this mutation.
                const resolve_ts = if (summary.finalized_at > 0) summary.finalized_at else resolution_timestamp;
                var cleanup_batch: ResolutionExtraBatch = .{};
                var initialized = false;
                defer if (initialized) {
                    if (extra_hooks.cleanup) |cleanup| cleanup(extra_hooks.ctx, cleanup_batch);
                };
                if (extra_hooks.build) |build| {
                    cleanup_batch = try build(extra_hooks.ctx, self, txn_id, .aborted, resolve_ts);
                    initialized = true;
                }
                _ = try self.resolveIntentsWithExtraBatch(txn_id, .aborted, resolve_ts, cleanup_batch);
                stats.resolved_finalized += 1;
                continue;
            }

            if (!(summary.intents_resolved_known and summary.intents_resolved) and try self.hasAnyIntents(txn_id)) {
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
            } else if (!(summary.intents_resolved_known and summary.intents_resolved)) {
                // A pre-v6 terminal record without intents is already locally
                // resolved. Persist the marker once so retained transaction
                // history does not repeat a broad compatibility scan.
                var upgraded = try self.loadTransactionRecord(txn_id);
                upgraded.intents_resolved = true;
                upgraded.intents_resolved_known = true;
                const upgraded_key = makeRecordKey(txn_id);
                const upgraded_value = try self.encodeRecord(upgraded);
                defer self.alloc.free(upgraded_value);
                const stale_manifest_key = makeSidecarKey(intent_keys_prefix, txn_id);
                const stale_lease_key = makeSidecarKey(schema_leases_prefix, txn_id);
                const stale_admission_key = makeSidecarKey(intent_admission_prefix, txn_id);
                const admission = try self.loadIntentAdmission(self.alloc, txn_id);
                try self.applyBatchWithReservation(&.{.{ .key = &upgraded_key, .value = upgraded_value }}, &.{ &stale_manifest_key, &stale_lease_key, &stale_admission_key }, null, .{ .previous = if (admission) |value| value.reservation() else 0, .next = 0 });
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
            const no_intents = if (refreshed.intents_resolved_known and refreshed.intents_resolved)
                true
            else
                !try self.hasAnyIntents(txn_id);
            if (refreshed.status != .pending and refreshed.finalized_at < cleanup_cutoff and
                no_intents and !try self.hasReplicationOutbox(txn_id))
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
        return self.checkVersionPredicatesInner(predicates, exclude_txn, null);
    }

    pub fn checkVersionPredicatesWithAdmission(self: *TxnManager, predicates: []const VersionPredicate, exclude_txn: ?TxnId, original: ?read_control.Port) !void {
        var trace: MutationAdmissionTrace = .{};
        return self.checkVersionPredicatesInner(predicates, exclude_txn, trace.bind(original)) catch |err| return trace.failure orelse err;
    }

    fn checkVersionPredicatesInner(
        self: *TxnManager,
        predicates: []const VersionPredicate,
        exclude_txn: ?TxnId,
        control: ?read_control.Port,
    ) !void {
        if (control) |owner| try owner.checkpoint();
        var relational_primary: ?bool = null;
        for (predicates) |pred| {
            if (control) |owner| try owner.checkpoint();
            if (pred.unique_absence and (pred.comparison != .document_version or pred.expected_version != 0)) return error.InvalidArgument;
            switch (pred.comparison) {
                .document_version => {
                    const current_ts = try self.readTimestampWithAdmission(pred.key, control);
                    if (pred.expected_version == 0) {
                        if (current_ts != null) return if (pred.unique_absence) error.UniqueConstraintViolation else TxnError.VersionConflict;
                    } else {
                        const ts = current_ts orelse return TxnError.VersionConflict;
                        if (ts != pred.expected_version) return TxnError.VersionConflict;
                    }
                    if (pred.expected_content_digest) |expected| {
                        if (pred.expected_version == 0) return error.InvalidArgument;
                        if (relational_primary == null) {
                            const table_catalog = @import("db/table_catalog.zig");
                            const catalog_bytes = self.getAllocWithAdmission(self.alloc, table_catalog.key, control) catch |err| switch (err) {
                                error.NotFound => null,
                                else => return err,
                            };
                            defer if (catalog_bytes) |bytes| self.alloc.free(bytes);
                            if (catalog_bytes) |bytes| {
                                const catalog = try table_catalog.Catalog.decode(bytes);
                                relational_primary = catalog.mode_initialized and catalog.storage_mode == .relational;
                            } else {
                                // Legacy document stores predate the fixed catalog.
                                if (control) |owner| try owner.checkpoint();
                                const schema = try @import("schema.zig").loadSchema(self.store, self.alloc);
                                defer if (schema) |value| @import("schema.zig").freeSchema(self.alloc, value);
                                if (control) |owner| try owner.checkpoint();
                                relational_primary = schema != null and schema.?.storage_mode == .relational;
                            }
                        }
                        const primary_key = if (relational_primary.?)
                            try @import("transaction_intent_admission.zig").userKey(self.alloc, pred.key, internal_keys.relational_row_kind, control)
                        else
                            try @import("transaction_intent_admission.zig").userKey(self.alloc, pred.key, internal_keys.primary_kind, control);
                        defer self.alloc.free(primary_key);
                        const current = self.getAllocWithAdmission(self.alloc, primary_key, control) catch |err| switch (err) {
                            error.NotFound => return TxnError.VersionConflict,
                            else => return err,
                        };
                        defer self.alloc.free(current);
                        const digest = try @import("transaction_intent_admission.zig").sha256(current, control);
                        if (!std.mem.eql(u8, &digest, &expected)) return TxnError.VersionConflict;
                    }
                },
                .exact_value => {
                    if (!isRawMetadataPredicateKey(pred.key)) return error.InvalidArgument;
                    if (range_protection.counterBucket(pred.key)) |id| {
                        var probe = if (control) |owner| try self.store.beginProbeWithAdmission(.retain, owner) else try self.store.beginProbe();
                        defer probe.abort();
                        if (!try range_protection.isActive(&probe)) return error.SqlRangeTrackingRequired;
                        var read = if (control) |owner| try self.store.beginCurrentScanWithAdmission(owner) else try self.store.beginCurrentScan();
                        defer read.abort();
                        var cursor = if (control) |owner| try read.openCursorWithAdmission(owner) else try read.openCursor();
                        defer cursor.close();
                        const writer_key = range_protection.writerKey(id);
                        try self.checkReadGuardCursorWithAdmission(&cursor, &writer_key, exclude_txn, control);
                    }
                    if (range_protection.indexCounterDigest(pred.key)) |digest| {
                        var probe = if (control) |owner| try self.store.beginProbeWithAdmission(.retain, owner) else try self.store.beginProbe();
                        defer probe.abort();
                        if (!try range_protection.isActive(&probe)) return error.SqlRangeTrackingRequired;
                        var read = if (control) |owner| try self.store.beginCurrentScanWithAdmission(owner) else try self.store.beginCurrentScan();
                        defer read.abort();
                        var cursor = if (control) |owner| try read.openCursorWithAdmission(owner) else try read.openCursor();
                        defer cursor.close();
                        const writer_key = range_protection.indexWriterKey(digest);
                        try self.checkReadGuardCursorWithAdmission(&cursor, &writer_key, exclude_txn, control);
                    }
                    const current = self.getAllocWithAdmission(self.alloc, pred.key, control) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    defer if (current) |bytes| self.alloc.free(bytes);
                    if (pred.expected_value) |expected| {
                        if (!try @import("transaction_intent_admission.zig").equalBytes(current orelse return TxnError.VersionConflict, expected, control)) return TxnError.VersionConflict;
                    } else if (current != null) return TxnError.VersionConflict;
                },
            }

            if (try self.hasPendingIntentForKeyWithAdmission(pred.key, exclude_txn, control)) {
                return TxnError.IntentConflict;
            }
        }
        if (control) |owner| try owner.checkpoint();
    }

    pub fn checkIntentConflicts(
        self: *TxnManager,
        intents: []const WriteIntent,
        exclude_txn: ?TxnId,
    ) !void {
        return self.checkIntentConflictsInner(intents, exclude_txn, null);
    }

    pub fn checkIntentConflictsWithAdmission(self: *TxnManager, intents: []const WriteIntent, exclude_txn: ?TxnId, original: ?read_control.Port) !void {
        var trace: MutationAdmissionTrace = .{};
        return self.checkIntentConflictsInner(intents, exclude_txn, trace.bind(original)) catch |err| return trace.failure orelse err;
    }

    fn checkIntentConflictsInner(
        self: *TxnManager,
        intents: []const WriteIntent,
        exclude_txn: ?TxnId,
        control: ?read_control.Port,
    ) !void {
        if (control) |owner| try owner.checkpoint();
        for (intents) |intent| {
            if (control) |owner| try owner.checkpoint();
            if (try self.hasPendingIntentForKeyWithAdmission(intent.key, exclude_txn, control)) {
                return TxnError.IntentConflict;
            }
        }
        if (try self.readGuardCountWithAdmission(control) != 0) {
            var scan = if (control) |owner| try self.store.beginCurrentScanWithAdmission(owner) else try self.store.beginCurrentScan();
            defer scan.abort();
            var cursor = if (control) |owner| try scan.openCursorWithAdmission(owner) else try scan.openCursor();
            defer cursor.close();
            var buckets: std.StaticBitSet(range_protection.bucket_count) = .empty;
            for (intents) |intent| {
                if (control) |owner| try owner.checkpoint();
                try self.checkReadGuardCursorWithAdmission(&cursor, intent.key, exclude_txn, control);
                if (std.mem.startsWith(u8, intent.key, "\x00\x00")) continue;
                const id = range_protection.bucket(intent.key);
                if (buckets.isSet(id)) continue;
                buckets.set(id);
                const counter = range_protection.counterKey(id);
                try self.checkReadGuardCursorWithAdmission(&cursor, &counter, exclude_txn, control);
            }
        }
        if (control) |owner| try owner.checkpoint();
    }

    /// Check if any other pending transaction has an intent on this key.
    fn hasPendingIntentForKey(self: *TxnManager, user_key: []const u8, exclude_txn: ?TxnId) !bool {
        return self.hasPendingIntentForKeyWithAdmission(user_key, exclude_txn, null);
    }

    fn hasPendingIntentForKeyWithAdmission(self: *TxnManager, user_key: []const u8, exclude_txn: ?TxnId, control: ?read_control.Port) !bool {
        const lock_key = try self.makeIntentLockKeyWithAdmission(user_key, control);
        defer self.alloc.free(lock_key);
        const owner = self.getAllocWithAdmission(self.alloc, lock_key, control) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        defer self.alloc.free(owner);
        if (owner.len != @sizeOf(TxnId)) return TxnError.InvalidTxnRecord;
        if (exclude_txn) |txn_id| if (std.mem.eql(u8, owner, &txn_id)) return false;
        return true;
    }

    pub fn checkOrdinaryWriteConflict(self: *TxnManager, key: []const u8) !void {
        try self.checkIntentConflicts(&.{.{ .key = key, .value = null }}, null);
    }

    /// Check a complete ordinary write batch with one sorted point probe.
    /// Intent locks are independent keys, so opening a read transaction per
    /// user key only multiplies snapshot/layout work without strengthening the
    /// conflict guarantee. The DB apply mutex keeps this batch check
    /// linearizable with concurrent intent admission.
    pub fn checkOrdinaryWriteConflicts(self: *TxnManager, user_keys: []const []const u8) !void {
        if (user_keys.len == 0) return;

        const lock_keys = try self.alloc.alloc([]u8, user_keys.len);
        var initialized: usize = 0;
        defer {
            for (lock_keys[0..initialized]) |key| self.alloc.free(key);
            self.alloc.free(lock_keys);
        }
        for (user_keys, 0..) |user_key, i| {
            lock_keys[i] = try self.makeIntentLockKey(user_key);
            initialized += 1;
        }
        std.mem.sort([]u8, lock_keys, {}, struct {
            fn lessThan(_: void, left: []u8, right: []u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        }.lessThan);

        const key_refs = try self.alloc.alloc([]const u8, lock_keys.len);
        defer self.alloc.free(key_refs);
        for (lock_keys, 0..) |key, i| key_refs[i] = key;
        const owners = try self.alloc.alloc(?[]const u8, key_refs.len);
        defer self.alloc.free(owners);
        @memset(owners, null);

        const have_readers = blk: {
            var probe = try self.store.beginProbe();
            defer probe.abort();
            try probe.getManySorted(key_refs, owners);
            for (owners) |maybe_owner| {
                const owner = maybe_owner orelse continue;
                if (owner.len != @sizeOf(TxnId)) return TxnError.InvalidTxnRecord;
                return TxnError.IntentConflict;
            }
            const raw = probe.get(read_guard_count_key) catch |err| switch (err) {
                error.NotFound => break :blk false,
                else => return err,
            };
            if (raw.len != 8 or std.mem.readInt(u64, raw[0..8], .little) == 0) return error.InvalidTxnRecord;
            break :blk true;
        };
        if (have_readers) {
            var scan = try self.store.beginCurrentScan();
            defer scan.abort();
            var cursor = try scan.openCursor();
            defer cursor.close();
            var buckets: std.StaticBitSet(range_protection.bucket_count) = .empty;
            for (user_keys) |key| {
                try self.checkReadGuardCursor(&cursor, key, null);
                if (std.mem.startsWith(u8, key, "\x00\x00")) continue;
                const id = range_protection.bucket(key);
                if (buckets.isSet(id)) continue;
                buckets.set(id);
                const counter = range_protection.counterKey(id);
                try self.checkReadGuardCursor(&cursor, &counter, null);
            }
        }
    }

    /// Forward-index effects are computed under the DB apply fence after the
    /// final row image is known. Check their exact tuple spans before the same
    /// atomic batch publishes the primary and index records.
    pub fn checkIndexForwardWriteConflicts(self: *TxnManager, physical_keys: []const []const u8) !void {
        if (physical_keys.len == 0 or try self.readGuardCount() == 0) return;
        var scan = try self.store.beginCurrentScan();
        defer scan.abort();
        var cursor = try scan.openCursor();
        defer cursor.close();
        var seen = std.AutoHashMapUnmanaged([range_protection.index_span_digest_bytes]u8, void).empty;
        defer seen.deinit(self.alloc);
        for (physical_keys) |key| {
            const digest = (try range_protection.indexSpanDigest(key)) orelse continue;
            if ((try seen.getOrPut(self.alloc, digest)).found_existing) continue;
            const counter = range_protection.indexCounterKey(digest);
            try self.checkReadGuardCursor(&cursor, &counter, null);
        }
    }

    fn checkIndexSpanGuardConflicts(
        self: *TxnManager,
        digests: []const [range_protection.index_span_digest_bytes]u8,
        exclude_txn: ?TxnId,
    ) !void {
        return self.checkIndexSpanGuardConflictsWithAdmission(digests, exclude_txn, null);
    }

    pub fn checkIndexSpanGuardConflictsWithAdmission(
        self: *TxnManager,
        digests: []const [range_protection.index_span_digest_bytes]u8,
        exclude_txn: ?TxnId,
        control: ?read_control.Port,
    ) !void {
        var trace: MutationAdmissionTrace = .{};
        const original = trace.bind(control);
        self.checkIndexSpanGuardConflictsInner(digests, exclude_txn, original) catch |err| return trace.failure orelse err;
        if (original) |owner| try owner.checkpoint();
    }

    fn checkIndexSpanGuardConflictsInner(
        self: *TxnManager,
        digests: []const [range_protection.index_span_digest_bytes]u8,
        exclude_txn: ?TxnId,
        control: ?read_control.Port,
    ) !void {
        if (control) |owner| try owner.checkpoint();
        if (digests.len == 0 or try self.readGuardCountWithAdmission(control) == 0) return;
        var scan = if (control) |owner| try self.store.beginCurrentScanWithAdmission(owner) else try self.store.beginCurrentScan();
        defer scan.abort();
        var cursor = if (control) |owner| try scan.openCursorWithAdmission(owner) else try scan.openCursor();
        defer cursor.close();
        for (digests) |digest| {
            if (control) |owner| try owner.checkpoint();
            const counter = range_protection.indexCounterKey(digest);
            try self.checkReadGuardCursorWithAdmission(&cursor, &counter, exclude_txn, control);
        }
    }

    fn readGuardCount(self: *TxnManager) !u64 {
        return self.readGuardCountWithAdmission(null);
    }

    fn readGuardCountWithAdmission(self: *TxnManager, control: ?read_control.Port) !u64 {
        const raw = self.getAllocWithAdmission(self.alloc, read_guard_count_key, control) catch |err| switch (err) {
            error.NotFound => return 0,
            else => return err,
        };
        defer self.alloc.free(raw);
        if (raw.len != 8) return error.InvalidTxnRecord;
        const count = std.mem.readInt(u64, raw[0..8], .little);
        if (count == 0) return error.InvalidTxnRecord;
        return count;
    }

    fn loadReadAdmission(self: *TxnManager, txn_id: TxnId) !IntentAdmission {
        return self.loadReadAdmissionWithAdmission(txn_id, null);
    }

    fn loadReadAdmissionWithAdmission(self: *TxnManager, txn_id: TxnId, control: ?read_control.Port) !IntentAdmission {
        const key = makeSidecarKey(read_admission_prefix, txn_id);
        const raw = self.getAllocWithAdmission(self.alloc, &key, control) catch |err| switch (err) {
            error.NotFound => return .{},
            else => return err,
        };
        defer self.alloc.free(raw);
        if (raw.len != 16) return error.InvalidTxnRecord;
        const result = IntentAdmission{ .count = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u64, raw[8..16], .little) };
        if (result.count == 0 or result.count > max_read_guards_per_transaction or result.bytes < result.count * 4096) return error.InvalidTxnRecord;
        return result;
    }

    fn makeReadGuardKey(alloc: Allocator, user_key: []const u8, txn_id: ?TxnId) ![]u8 {
        return makeReadGuardKeyWithAdmission(alloc, user_key, txn_id, null);
    }

    fn makeReadGuardKeyWithAdmission(alloc: Allocator, user_key: []const u8, txn_id: ?TxnId, control: ?read_control.Port) ![]u8 {
        const length = std.math.cast(u32, user_key.len) orelse return error.TransactionTooLarge;
        var length_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &length_bytes, length, .big);
        return @import("transaction_intent_admission.zig").concatKey(alloc, &.{ read_guards_prefix, &length_bytes, user_key, if (txn_id) |*id| id else "" }, control);
    }

    /// At most two entries: our own shared guard and the first other reader.
    /// Exact length framing prevents a guard for "a" locking "a\x00" or "ab".
    fn checkReadGuardCursor(self: *TxnManager, cursor: anytype, key: []const u8, exclude_txn: ?TxnId) !void {
        return self.checkReadGuardCursorWithAdmission(cursor, key, exclude_txn, null);
    }

    fn checkReadGuardCursorWithAdmission(self: *TxnManager, cursor: anytype, key: []const u8, exclude_txn: ?TxnId, control: ?read_control.Port) !void {
        const prefix = try makeReadGuardKeyWithAdmission(self.alloc, key, null, control);
        defer self.alloc.free(prefix);
        var entry = if (control) |owner| try cursor.seekAtOrAfterWithAdmission(prefix, owner) else try cursor.seekAtOrAfter(prefix);
        while (entry) |item| : (entry = if (control) |owner| try cursor.nextWithAdmission(owner) else try cursor.next()) {
            if (!try @import("transaction_intent_admission.zig").hasPrefix(item.key, prefix, control)) break;
            if (item.key.len != prefix.len + 16 or item.value.len != 0) return error.InvalidTxnRecord;
            if (exclude_txn) |id| if (std.mem.eql(u8, item.key[prefix.len..], &id)) continue;
            return error.IntentConflict;
        }
    }

    const ReadAdmissionChange = struct { previous: IntentAdmission, next: IntentAdmission };

    /// An implicit dependency may reuse a stronger caller-owned read guard.
    /// Do not replace its persisted version/digest identity on a later prepare.
    pub fn hasSharedReadDependency(self: *TxnManager, txn_id: TxnId, user_key: []const u8) !bool {
        const prefix = makeSidecarKey(read_members_prefix, txn_id);
        const key = try std.mem.concat(self.alloc, u8, &.{ &prefix, user_key });
        defer self.alloc.free(key);
        const member = self.getAlloc(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        defer self.alloc.free(member);
        if (!validReadMember(member)) return error.InvalidTxnRecord;
        return true;
    }

    fn stageReadGuards(
        self: *TxnManager,
        txn_id: TxnId,
        predicates: []const VersionPredicate,
        write_set: *const std.StringHashMapUnmanaged(usize),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        writes: *std.ArrayListUnmanaged(docstore.KVPair),
        control: ?read_control.Port,
    ) !ReadAdmissionChange {
        if (control) |owner| try owner.checkpoint();
        if (predicates.len > max_read_guards_per_transaction) return error.TransactionTooLarge;
        const previous = try self.loadReadAdmissionWithAdmission(txn_id, control);
        var next = previous;
        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(self.alloc);
        const member_prefix = makeSidecarKey(read_members_prefix, txn_id);
        for (predicates) |predicate| {
            if (control) |owner| try owner.checkpoint();
            if (write_set.contains(predicate.key) or (try seen.getOrPut(self.alloc, predicate.key)).found_existing) continue;
            const member = try @import("transaction_intent_admission.zig").concatKey(self.alloc, &.{ &member_prefix, predicate.key }, control);
            try appendOwnedBytes(self.alloc, owned_keys, member);
            const old = self.getAllocWithAdmission(self.alloc, member, control) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (old) |bytes| self.alloc.free(bytes);
            var identity_buf: [33]u8 = undefined;
            const identity = try readPredicateIdentityWithAdmission(predicate, &identity_buf, control);
            if (old) |bytes| {
                if (!validReadMember(bytes)) return error.InvalidTxnRecord;
                if (!std.mem.eql(u8, bytes, identity)) return error.VersionConflict;
                continue;
            }
            next.count = std.math.add(u64, next.count, 1) catch return error.TransactionTooLarge;
            if (next.count > max_read_guards_per_transaction) return error.TransactionTooLarge;
            next.bytes = std.math.add(u64, next.bytes, try intentAdmissionBytesWithAdmission(.{ .key = predicate.key, .value = null }, control)) catch return error.TransactionTooLarge;
            const version = try self.alloc.dupe(u8, identity);
            try appendOwnedBytes(self.alloc, owned_values, version);
            try writes.append(self.alloc, .{ .key = member, .value = version });
            const guard = try makeReadGuardKeyWithAdmission(self.alloc, predicate.key, txn_id, control);
            try appendOwnedBytes(self.alloc, owned_keys, guard);
            try writes.append(self.alloc, .{ .key = guard, .value = "" });
        }
        if (next.count != previous.count) {
            const admission_key = try self.alloc.dupe(u8, &makeSidecarKey(read_admission_prefix, txn_id));
            try appendOwnedBytes(self.alloc, owned_keys, admission_key);
            const admission_value = try self.alloc.alloc(u8, 16);
            try appendOwnedBytes(self.alloc, owned_values, admission_value);
            std.mem.writeInt(u64, admission_value[0..8], next.count, .little);
            std.mem.writeInt(u64, admission_value[8..16], next.bytes, .little);
            try writes.append(self.alloc, .{ .key = admission_key, .value = admission_value });
            const count = std.math.add(u64, try self.readGuardCountWithAdmission(control), next.count - previous.count) catch return error.TransactionTooLarge;
            const count_value = try self.alloc.alloc(u8, 8);
            try appendOwnedBytes(self.alloc, owned_values, count_value);
            std.mem.writeInt(u64, count_value[0..8], count, .little);
            try writes.append(self.alloc, .{ .key = read_guard_count_key, .value = count_value });
        }
        if (control) |owner| try owner.checkpoint();
        return .{ .previous = previous, .next = next };
    }

    /// Build an intent key: intents_prefix + txn_id + ':' + user_key
    fn makeIntentKey(self: *TxnManager, txn_id: TxnId, user_key: []const u8) ![]u8 {
        return self.makeIntentKeyWithAdmission(txn_id, user_key, null);
    }

    fn makeIntentKeyWithAdmission(self: *TxnManager, txn_id: TxnId, user_key: []const u8, control: ?read_control.Port) ![]u8 {
        return @import("transaction_intent_admission.zig").concatKey(self.alloc, &.{ intents_prefix, &txn_id, ":", user_key }, control);
    }

    fn makeIntentLockKey(self: *TxnManager, user_key: []const u8) ![]u8 {
        return self.makeIntentLockKeyWithAdmission(user_key, null);
    }

    fn makeIntentLockKeyWithAdmission(self: *TxnManager, user_key: []const u8, control: ?read_control.Port) ![]u8 {
        return @import("transaction_intent_admission.zig").concatKey(self.alloc, &.{ intent_locks_prefix, user_key }, control);
    }

    fn loadTransactionRecord(self: *TxnManager, txn_id: TxnId) !TxnRecord {
        return self.loadTransactionRecordWithAdmission(txn_id, null);
    }

    fn loadTransactionRecordWithAdmission(self: *TxnManager, txn_id: TxnId, control: ?read_control.Port) !TxnRecord {
        const key = makeRecordKey(txn_id);
        const val = self.getAllocWithAdmission(self.alloc, &key, control) catch |err| switch (err) {
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
        const buf = try self.alloc.alloc(u8, if (record.preparation_binding != null) txn_record_v8_size else if (record.preparation_requirement != null) txn_record_v7_size else txn_record_v6_size);
        buf[0] = @backingInt(record.status);
        std.mem.writeInt(u64, buf[1..9], record.begin_timestamp, .little);
        std.mem.writeInt(u64, buf[9..17], record.commit_version, .little);
        std.mem.writeInt(u64, buf[17..25], record.created_at, .little);
        std.mem.writeInt(u64, buf[25..33], record.finalized_at, .little);
        std.mem.writeInt(u64, buf[33..41], record.intent_revision, .little);
        std.mem.writeInt(u64, buf[41..49], record.replay_sequence, .little);
        buf[49] = @intFromBool(record.prepared);
        buf[50] = @intFromBool(record.coordinator);
        buf[51] = @intFromBool(record.retain_terminal);
        buf[52] = @intFromBool(record.intents_resolved);
        if (record.preparation_requirement) |required| buf[53] = switch (required.mode) {
            .deferred => 1,
            .synchronous => 2,
        };
        if (record.preparation_binding) |binding| {
            if (record.preparation_requirement == null) {
                self.alloc.free(buf);
                return error.InvalidTxnRecord;
            }
            const raw = binding.encode() catch |err| {
                self.alloc.free(buf);
                return err;
            };
            buf[txn_record_v7_size..][0..prepared_effects.Binding.encoded_bytes].* = raw;
        }
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
        return try self.cleanupTransactionMetadataIfEligibleExtraBatch(
            txn_id,
            cutoff_timestamp,
            retained_cutoff_timestamp,
            .{},
        );
    }

    pub fn cleanupTransactionMetadataIfEligibleExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
        extra_batch: MutationExtraBatch,
    ) !bool {
        const record = self.loadTransactionRecord(txn_id) catch |err| switch (err) {
            TxnError.TxnNotFound => {
                try self.applyMutationExtraBatch(extra_batch);
                return false;
            },
            else => return err,
        };
        const has_intents = if (record.intents_resolved_known and record.intents_resolved)
            false
        else
            try self.hasAnyIntents(txn_id);
        if (record.status == .pending or has_intents or try self.hasReplicationOutbox(txn_id) or try self.hasPreparedEffects(txn_id) or try self.hasPreparationRequirement(txn_id)) {
            try self.applyMutationExtraBatch(extra_batch);
            return false;
        }
        const cutoff = if (record.retain_terminal) retained_cutoff_timestamp else cutoff_timestamp;
        if (record.finalized_at >= cutoff) {
            try self.applyMutationExtraBatch(extra_batch);
            return false;
        }
        const unresolved = try self.getUnresolvedParticipants(self.alloc, txn_id);
        defer freeParticipantList(self.alloc, unresolved);
        if (unresolved.len != 0) {
            try self.applyMutationExtraBatch(extra_batch);
            return false;
        }
        try self.deleteTransactionMetadataExtraBatch(txn_id, extra_batch);
        return true;
    }

    fn hasAnyIntents(self: *TxnManager, txn_id: TxnId) !bool {
        return try self.hasIntents(txn_id);
    }

    fn loadIntentKeysOptional(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !?[][]u8 {
        return self.loadIntentKeysOptionalWithAdmission(alloc, txn_id, null);
    }

    fn loadIntentKeysOptionalWithAdmission(self: *TxnManager, alloc: Allocator, txn_id: TxnId, control: ?read_control.Port) !?[][]u8 {
        if (control) |owner| try owner.checkpoint();
        if (try self.loadIntentAdmission(alloc, txn_id)) |admission| {
            const prefix = try makeIntentMemberKey(alloc, txn_id, "");
            defer alloc.free(prefix);
            var keys = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (keys.items) |key| alloc.free(key);
                keys.deinit(alloc);
            }
            var scan = if (control) |owner| try self.store.beginCurrentScanWithAdmission(owner) else try self.store.beginCurrentScan();
            defer scan.abort();
            var cursor = if (control) |owner| try scan.openCursorWithAdmission(owner) else try scan.openCursor();
            defer cursor.close();
            var entry = if (control) |owner| try cursor.seekAtOrAfterWithAdmission(prefix, owner) else try cursor.seekAtOrAfter(prefix);
            while (entry) |item| {
                if (!std.mem.startsWith(u8, item.key, prefix)) break;
                {
                    const key = try backend_scan.dupeWithAdmission(alloc, item.key[prefix.len..], control);
                    errdefer alloc.free(key);
                    try keys.append(alloc, key);
                }
                if (control) |owner| try owner.checkpoint();
                entry = if (control) |owner| try cursor.nextWithAdmission(owner) else try cursor.next();
            }
            if (keys.items.len != admission.count) return error.IntentSnapshotChanged;
            return try keys.toOwnedSlice(alloc);
        }
        const key = makeSidecarKey(intent_keys_prefix, txn_id);
        const raw = self.getAlloc(alloc, &key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(raw);
        return try decodeParticipantListWithAdmission(alloc, raw, control);
    }

    fn loadIntentAdmission(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !?IntentAdmission {
        return self.loadIntentAdmissionWithAdmission(alloc, txn_id, null);
    }

    fn loadIntentAdmissionWithAdmission(self: *TxnManager, alloc: Allocator, txn_id: TxnId, control: ?read_control.Port) !?IntentAdmission {
        const key = makeSidecarKey(intent_admission_prefix, txn_id);
        const raw = self.getAllocWithAdmission(alloc, &key, control) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(raw);
        if (raw.len != 16 and raw.len != 32) return error.InvalidTxnRecord;
        return .{ .count = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u64, raw[8..16], .little), .retained_bytes = if (raw.len == 32) std.mem.readInt(u64, raw[16..24], .little) else 0, .retained_keys = if (raw.len == 32) std.mem.readInt(u64, raw[24..32], .little) else 0, .retention_tracked = raw.len == 32 };
    }

    fn makeIntentMemberKey(alloc: Allocator, txn_id: TxnId, key: []const u8) ![]u8 {
        return makeIntentMemberKeyWithAdmission(alloc, txn_id, key, null);
    }

    fn makeIntentMemberKeyWithAdmission(alloc: Allocator, txn_id: TxnId, key: []const u8, control: ?read_control.Port) ![]u8 {
        return @import("transaction_intent_admission.zig").concatKey(alloc, &.{ intent_members_prefix, &txn_id, ":", key }, control);
    }

    fn loadIntentEntries(
        self: *TxnManager,
        alloc: Allocator,
        txn_id: TxnId,
        scan_prefix: []const u8,
    ) ![]backend_scan.OwnedKVPair {
        return self.loadIntentEntriesWithAdmission(alloc, txn_id, scan_prefix, null);
    }

    fn loadIntentEntriesWithAdmission(
        self: *TxnManager,
        alloc: Allocator,
        txn_id: TxnId,
        scan_prefix: []const u8,
        control: ?read_control.Port,
    ) ![]backend_scan.OwnedKVPair {
        if (control) |owner| try owner.checkpoint();
        const intent_keys = (try self.loadIntentKeysOptionalWithAdmission(alloc, txn_id, control)) orelse
            return try backend_scan.scanPrefixWithAdmission(alloc, &self.store, scan_prefix, control);
        defer freeParticipantList(alloc, intent_keys);
        return try self.loadIntentEntriesByKeysWithAdmission(alloc, scan_prefix, intent_keys, control);
    }

    fn loadIntentRetirementEntries(self: *TxnManager, alloc: Allocator, txn_id: TxnId, prefix: []const u8) ![]backend_scan.OwnedKVPair {
        if (try self.loadIntentKeysOptional(alloc, txn_id)) |keys| {
            defer freeParticipantList(alloc, keys);
            return try intentRetirementEntries(alloc, prefix, keys);
        }
        return try self.scanPrefix(alloc, prefix);
    }

    fn loadIntentEntriesByKeys(
        self: *TxnManager,
        alloc: Allocator,
        scan_prefix: []const u8,
        user_keys: []const []const u8,
    ) ![]backend_scan.OwnedKVPair {
        return self.loadIntentEntriesByKeysWithAdmission(alloc, scan_prefix, user_keys, null);
    }

    fn loadIntentEntriesByKeysWithAdmission(
        self: *TxnManager,
        alloc: Allocator,
        scan_prefix: []const u8,
        user_keys: []const []const u8,
        control: ?read_control.Port,
    ) ![]backend_scan.OwnedKVPair {
        if (control) |owner| try owner.checkpoint();
        if (user_keys.len == 0) return try alloc.alloc(backend_scan.OwnedKVPair, 0);

        const full_keys = try alloc.alloc([]u8, user_keys.len);
        var full_keys_initialized: usize = 0;
        defer {
            for (full_keys[0..full_keys_initialized]) |key| alloc.free(key);
            alloc.free(full_keys);
        }
        for (user_keys, 0..) |user_key, i| {
            if (control) |owner| try owner.checkpoint();
            const combined = try alloc.alloc(u8, try std.math.add(usize, scan_prefix.len, user_key.len));
            errdefer alloc.free(combined);
            @memcpy(combined[0..scan_prefix.len], scan_prefix);
            var offset: usize = 0;
            while (offset < user_key.len) {
                if (control) |owner| try owner.checkpoint();
                const end = offset + @min(user_key.len - offset, 64 * 1024);
                @memcpy(combined[scan_prefix.len + offset ..][0 .. end - offset], user_key[offset..end]);
                offset = end;
            }
            full_keys[i] = combined;
            full_keys_initialized += 1;
        }
        try read_control.sort([]u8, full_keys, {}, struct {
            fn lessThan(_: void, left: []u8, right: []u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        }.lessThan, control);

        const key_refs = try alloc.alloc([]const u8, full_keys.len);
        defer alloc.free(key_refs);
        for (full_keys, 0..) |key, i| {
            if (i % 64 == 0) if (control) |owner| try owner.checkpoint();
            key_refs[i] = key;
        }
        const values = try alloc.alloc(?[]const u8, full_keys.len);
        defer alloc.free(values);
        @memset(values, null);
        var probe = if (control) |owner| try self.store.beginProbeWithAdmission(.retain, owner) else try self.store.beginProbe();
        defer probe.abort();
        if (control == null) try probe.getManySorted(key_refs, values);

        const entries = try alloc.alloc(backend_scan.OwnedKVPair, full_keys.len);
        var initialized: usize = 0;
        errdefer {
            for (entries[0..initialized]) |entry| {
                alloc.free(entry.key);
                alloc.free(entry.value);
            }
            alloc.free(entries);
        }
        var start: usize = 0;
        while (start < values.len) {
            const end = if (control != null) start + @min(values.len - start, 64) else values.len;
            if (control) |owner| {
                try owner.checkpoint();
                try probe.getManySortedWithAdmission(key_refs[start..end], values[start..end], owner);
                try owner.checkpoint();
            }
            // Copy each probe batch before another probe can reuse scratch.
            for (values[start..end], start..) |maybe_value, i| {
                const value = maybe_value orelse return error.IntentSnapshotChanged;
                const owned_key = try backend_scan.dupeWithAdmission(alloc, key_refs[i], control);
                errdefer alloc.free(owned_key);
                const owned_value = try backend_scan.dupeWithAdmission(alloc, value, control);
                entries[i] = .{
                    .key = owned_key,
                    .value = owned_value,
                };
                initialized += 1;
            }
            start = end;
        }
        if (control) |owner| try owner.checkpoint();
        return entries;
    }

    /// A revision-fenced caller has already materialized these intents. Only
    /// their keys are needed to atomically retire intent rows and locks.
    fn intentRetirementEntries(alloc: Allocator, prefix: []const u8, keys: []const []const u8) ![]backend_scan.OwnedKVPair {
        const entries = try alloc.alloc(backend_scan.OwnedKVPair, keys.len);
        var initialized: usize = 0;
        errdefer {
            for (entries[0..initialized]) |entry| alloc.free(entry.key);
            alloc.free(entries);
        }
        for (keys, 0..) |key, i| {
            entries[i] = .{ .key = try std.mem.concat(alloc, u8, &.{ prefix, key }), .value = &.{} };
            initialized += 1;
        }
        return entries;
    }

    fn scanPrefix(self: *TxnManager, alloc: Allocator, prefix: []const u8) ![]backend_scan.OwnedKVPair {
        return try backend_scan.scanPrefix(alloc, &self.store, prefix);
    }

    fn deleteTransactionMetadata(self: *TxnManager, txn_id: TxnId) !void {
        try self.deleteTransactionMetadataExtraBatch(txn_id, .{});
    }

    fn deleteTransactionMetadataExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        extra_batch: MutationExtraBatch,
    ) !void {
        const record_key = makeRecordKey(txn_id);
        const participant_key = makeSidecarKey(participants_prefix, txn_id);
        const resolved_key = makeSidecarKey(resolved_participants_prefix, txn_id);
        const hot_standby_batch_key = makeTransactionReplicationBatchOutboxKey(txn_id);
        const hot_standby_replay_key = makeTransactionReplicationReplayOutboxKey(txn_id);
        const publication_credit_key = publication_backlog.transactionCreditKey(txn_id);
        const prepared_key = makePreparedEffectsKey(txn_id);
        const requirement_key = makePreparationRequirementKey(txn_id);
        const intent_keys_key = makeSidecarKey(intent_keys_prefix, txn_id);
        const schema_lease_key = makeSidecarKey(schema_leases_prefix, txn_id);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);
        const admission_key = makeSidecarKey(intent_admission_prefix, txn_id);
        try deletes.append(self.alloc, &admission_key);
        try deletes.appendSlice(self.alloc, &.{ &record_key, &participant_key, &resolved_key, &hot_standby_batch_key, &hot_standby_replay_key, &publication_credit_key, &intent_keys_key, &schema_lease_key, &prepared_key, &requirement_key });
        try deletes.appendSlice(self.alloc, extra_batch.deletes);
        const member_prefix = makeSidecarKey(participant_index_prefix, txn_id);
        const resolution_prefix = makeSidecarKey(resolved_index_prefix, txn_id);
        const members = try self.scanPrefix(self.alloc, &member_prefix);
        defer backend_scan.freeResults(self.alloc, members);
        const resolutions = try self.scanPrefix(self.alloc, &resolution_prefix);
        defer backend_scan.freeResults(self.alloc, resolutions);
        for (members) |member| try deletes.append(self.alloc, member.key);
        for (resolutions) |resolution| try deletes.append(self.alloc, resolution.key);
        try self.applyBatch(extra_batch.writes, deletes.items, null);
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
        return self.loadParticipantSetWithAdmission(alloc, prefix, txn_id, null);
    }

    fn loadParticipantSetWithAdmission(self: *TxnManager, alloc: Allocator, comptime prefix: []const u8, txn_id: TxnId, control: ?read_control.Port) ![][]u8 {
        const key = makeSidecarKey(prefix, txn_id);
        const raw = self.getAllocWithAdmission(alloc, &key, control) catch |err| switch (err) {
            error.NotFound => return alloc.alloc([]u8, 0),
            else => return err,
        };
        defer alloc.free(raw);
        return try decodeParticipantListWithAdmission(alloc, raw, control);
    }

    pub fn getAlloc(self: *TxnManager, alloc: Allocator, key: []const u8) ![]u8 {
        return self.getAllocWithAdmission(alloc, key, null);
    }

    fn getAllocWithAdmission(self: *TxnManager, alloc: Allocator, key: []const u8, control: ?read_control.Port) ![]u8 {
        // These helpers copy one value and release it immediately; none of
        // their callers retain a multi-operation snapshot. On the runtime LSM
        // a bound read clones the mutable memtable, while a probe reads the
        // current tip under the backend lock. In particular, ordinary batch
        // admission calls this for every transaction-intent lock key.
        var txn = if (control) |owner| try self.store.beginProbeWithAdmission(.retain, owner) else try self.store.beginProbe();
        defer txn.abort();
        const value = try txn.get(key);
        return try backend_scan.dupeWithAdmission(alloc, value, control);
    }

    fn keyExists(self: *TxnManager, key: []const u8) !bool {
        var txn = try self.store.beginProbe();
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

    fn applyMutationExtraBatch(self: *TxnManager, extra_batch: MutationExtraBatch) !void {
        return self.applyMutationExtraBatchWithAdmission(extra_batch, null);
    }

    fn applyMutationExtraBatchWithAdmission(self: *TxnManager, extra_batch: MutationExtraBatch, control: ?read_control.Port) !void {
        if (control) |owner| try owner.checkpoint();
        if (extra_batch.writes.len == 0 and extra_batch.deletes.len == 0) return;
        try self.applyBatchWithAdmission(extra_batch.writes, extra_batch.deletes, null, control);
    }

    fn applyBatch(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend) !void {
        return self.applyBatchWithAdmission(writes, deletes, replay, null);
    }

    fn applyBatchWithAdmission(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, control: ?read_control.Port) !void {
        return self.applyBatchWithParticipantAndAdmission(writes, deletes, replay, null, null, control);
    }

    const ReservationChange = struct { previous: u64, next: u64 };
    fn applyBatchWithReservation(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange) !void {
        return self.applyBatchWithParticipant(writes, deletes, replay, reservation, null);
    }

    fn applyBatchWithParticipant(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange, participant: ?@import("commit_participant.zig").Participant) !void {
        return self.applyBatchWithParticipantAndAdmission(writes, deletes, replay, reservation, participant, null);
    }

    fn applyPreparedSealBatch(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, reservation: ?backend_erased.PreparedReplayReservation, control: ?read_control.Port) !void {
        const plan = reservation orelse return self.applyBatchWithAdmission(writes, deletes, null, control);
        if (control) |owner| try owner.checkpoint();
        var batch = if (control) |owner| try self.store.beginBatchWithAdmission(owner) else try self.store.beginBatch();
        errdefer batch.abort();
        try @import("transaction_batch_admission.zig").stage(&batch, writes, deletes, @as(?ReplayAppend, null), control);
        try batch.reservePreparedReplay(plan, control);
        if (control) |owner| try owner.checkpoint();
        try batch.commit();
    }

    fn applyBatchWithParticipantAndAdmission(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange, participant: ?@import("commit_participant.zig").Participant, control: ?read_control.Port) !void {
        return self.applyResolutionResourceBatch(writes, deletes, replay, reservation, participant, control, null);
    }

    fn applyResolutionResourceBatch(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange, participant: ?@import("commit_participant.zig").Participant, control: ?read_control.Port, transition: ?struct { id: TxnId, status: TxnStatus }) !void {
        if (control) |owner| try owner.checkpoint();
        var batch = if (control) |owner| try self.store.beginBatchWithAdmission(owner) else try self.store.beginBatch();
        errdefer batch.abort();
        if (transition) |decision| {
            // The authoritative decision was checked before this writer opened.
            // Transition the grant before the frame deletion, in this same batch.
            const grant_key = @import("prepared_replay_credit.zig").grantKey(decision.id);
            const has_grant = present: {
                _ = batch.get(&grant_key) catch |err| {
                    if (err == error.NotFound) break :present false;
                    return err;
                };
                break :present true;
            };
            if (has_grant) switch (decision.status) {
                .aborted => try batch.cancelPreparedReplay(decision.id),
                .committed => {
                    // A raw adapter must not copy imported grant metadata
                    // while bypassing its owner's materialization/accounting.
                    if (batch.vtable.reserve_prepared_replay == null or batch.vtable.cancel_prepared_replay == null)
                        return error.Unsupported;
                    // Preserve an exact HA obligation even when a synchronous
                    // publisher is temporarily absent. Outbox deletion is its
                    // separate acknowledgement; local replay consumption is not.
                    const entry = replay orelse return error.PreparedEffectsRequired;
                    var physical_plan = try @import("prepared_replay_plan.zig").deriveFromMutations(self.alloc, writes, deletes, entry.payload, control);
                    defer physical_plan.deinit();
                    try @import("prepared_replay_credit.zig").validateMutationPlan(&batch, decision.id, physical_plan, control);
                    const key = makeTransactionReplicationReplayOutboxKey(decision.id);
                    try batch.put(&key, entry.payload);
                },
                .pending => return error.PreparedEffectsRequired,
            };
        }
        if (participant) |observer| try batch.setCommitParticipant(observer);
        if (reservation) |change| try retained_effects.replaceReservation(&batch, change.previous, change.next);
        try @import("transaction_batch_admission.zig").apply(&batch, writes, deletes, replay, control);
    }

    fn traceResolveDecisionConflict(self: *TxnManager, txn_id: TxnId, previous: TxnStatus, requested: TxnStatus, timestamp: u64) void {
        if (self.trace_writer) |tw| tw.traceEvent(&.{
            .name = "ResolveDecisionConflict",
            .txn_id = txn_id,
            .shard_id = self.shard_id,
            .timestamp = timestamp,
            .reason = resolveDecisionConflictReason(previous, requested),
        });
    }

    fn traceWriteIntentSuccess(self: *TxnManager, txn_id: TxnId, intents: []const WriteIntent, predicates: []const VersionPredicate) void {
        const tw = self.trace_writer orelse return;
        emitWriteIntentTrace(self.alloc, tw, txn_id, self.shard_id, "WriteIntentOnShard", intents, predicates, null);
    }

    fn traceWriteIntentFails(self: *TxnManager, txn_id: TxnId, intents: []const WriteIntent, reason: []const u8) void {
        const tw = self.trace_writer orelse return;
        emitWriteIntentTrace(self.alloc, tw, txn_id, self.shard_id, "WriteIntentFails", intents, &.{}, reason);
    }

    fn readTimestamp(self: *TxnManager, key: []const u8) !?u64 {
        return self.readTimestampWithAdmission(key, null);
    }

    fn readTimestampWithAdmission(self: *TxnManager, key: []const u8, control: ?read_control.Port) !?u64 {
        const ts_key = try @import("transaction_intent_admission.zig").userKey(self.alloc, key, internal_keys.ttl_kind, control);
        defer self.alloc.free(ts_key);
        const val = self.getAllocWithAdmission(self.alloc, ts_key, control) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(val);
        if (val.len < 8) return null;
        return std.mem.readInt(u64, val[0..8], .little);
    }
};

fn emitWriteIntentTrace(
    alloc: std.mem.Allocator,
    tw: tracing.AntflyTraceWriter,
    txn_id: TxnId,
    shard_id: []const u8,
    name: []const u8,
    intents: []const WriteIntent,
    predicates: []const VersionPredicate,
    reason: ?[]const u8,
) void {
    // Trace evidence must be complete. Failing the trace-enabled run is safer
    // than silently validating a transaction with omitted keys.
    var write_count: usize = 0;
    for (intents) |intent| {
        if (intent.value != null) write_count += 1;
    }
    const predicate_count = std.math.add(usize, intents.len, predicates.len) catch @panic("transaction trace key count overflow");
    const total = std.math.add(usize, intents.len, predicate_count) catch @panic("transaction trace key count overflow");
    const keys = alloc.alloc([]const u8, total) catch @panic("out of memory collecting transaction trace keys");
    defer alloc.free(keys);
    const write_keys = keys[0..write_count];
    const delete_keys = keys[write_count..intents.len];
    const predicate_keys = keys[intents.len..];
    var wk: usize = 0;
    var dk: usize = 0;
    for (intents) |intent| {
        if (intent.value != null) {
            write_keys[wk] = intent.key;
            wk += 1;
        } else {
            delete_keys[dk] = intent.key;
            dk += 1;
        }
    }
    // TLA+ TxnReadSet also includes write keys because admission checks them
    // for conflicting intents. Keep every explicit predicate and intent key.
    var pk: usize = 0;
    for (predicates) |predicate| {
        predicate_keys[pk] = predicate.key;
        pk += 1;
    }
    for (intents) |intent| {
        predicate_keys[pk] = intent.key;
        pk += 1;
    }
    tw.traceEvent(&.{
        .name = name,
        .txn_id = txn_id,
        .shard_id = shard_id,
        .write_keys = write_keys,
        .delete_keys = delete_keys,
        .predicate_keys = predicate_keys,
        .reason = reason,
    });
}

test "transaction trace retains every key beyond the former 32-key limit" {
    const Capture = struct {
        write_count: usize = 0,
        delete_count: usize = 0,
        predicate_count: usize = 0,
        last_predicate: []const u8 = "",

        fn onTrace(ptr: *anyopaque, event: *const tracing.AntflyTracingEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.write_count = event.write_keys.len;
            self.delete_count = event.delete_keys.len;
            self.predicate_count = event.predicate_keys.len;
            self.last_predicate = event.predicate_keys[event.predicate_keys.len - 1];
        }
    };
    var capture = Capture{};
    const writer = tracing.AntflyTraceWriter{
        .ptr = &capture,
        .vtable = &.{ .trace_event = Capture.onTrace },
    };
    var key_buffers: [65][16]u8 = undefined;
    var intents: [65]WriteIntent = undefined;
    for (&intents, 0..) |*intent, index| {
        const key = try std.fmt.bufPrint(&key_buffers[index], "key-{d}", .{index});
        intent.* = .{ .key = key, .value = if (index % 2 == 0) "value" else null };
    }
    var predicates: [5]VersionPredicate = undefined;
    for (&predicates, 0..) |*predicate, index| predicate.* = .{ .key = intents[index].key };

    emitWriteIntentTrace(std.testing.allocator, writer, @splat(0x42), "local", "WriteIntentOnShard", &intents, &predicates, null);
    try std.testing.expectEqual(@as(usize, 33), capture.write_count);
    try std.testing.expectEqual(@as(usize, 32), capture.delete_count);
    try std.testing.expectEqual(@as(usize, 70), capture.predicate_count);
    try std.testing.expectEqualStrings("key-64", capture.last_predicate);

    emitWriteIntentTrace(std.testing.allocator, writer, @splat(0x42), "local", "WriteIntentFails", &intents, &.{}, "IntentConflict");
    try std.testing.expectEqual(@as(usize, 65), capture.predicate_count);
    try std.testing.expectEqualStrings("key-64", capture.last_predicate);
}

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

const ParticipantIndex = struct {
    enlisted: u64 = 0,
    resolved: u64 = 0,
    fn decode(bytes: []const u8) !ParticipantIndex {
        if (bytes.len != 17 or bytes[0] != 1) return error.InvalidTxnRecord;
        const result: ParticipantIndex = .{ .enlisted = std.mem.readInt(u64, bytes[1..9], .little), .resolved = std.mem.readInt(u64, bytes[9..17], .little) };
        if (result.resolved > result.enlisted) return error.InvalidTxnRecord;
        return result;
    }
    fn encode(self: ParticipantIndex, bytes: *[17]u8) void {
        bytes[0] = 1;
        std.mem.writeInt(u64, bytes[1..9], self.enlisted, .little);
        std.mem.writeInt(u64, bytes[9..17], self.resolved, .little);
    }
};

fn makeParticipantIndexKey(comptime prefix: []const u8, txn_id: TxnId, participant: []const u8) [prefix.len + 16 + 32]u8 {
    var key: [prefix.len + 16 + 32]u8 = undefined;
    @memcpy(key[0..prefix.len], prefix);
    @memcpy(key[prefix.len..][0..16], &txn_id);
    std.crypto.hash.sha2.Sha256.hash(participant, key[prefix.len + 16 ..][0..32], .{});
    return key;
}

fn resolvedParticipantsInRead(alloc: Allocator, read: *backend_erased.ReadTxn, txn_id: TxnId) ![][]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var names: std.StringHashMapUnmanaged(void) = .empty;
    const legacy_key = makeSidecarKey(resolved_participants_prefix, txn_id);
    const legacy = read.get(&legacy_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (legacy) |bytes| for (try decodeParticipantList(scratch, bytes)) |name| try names.put(scratch, name, {});
    const prefix = makeSidecarKey(resolved_index_prefix, txn_id);
    var cursor = try read.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(&prefix);
    var count: u64 = 0;
    while (entry) |row| {
        if (!std.mem.startsWith(u8, row.key, &prefix)) break;
        const expected = makeParticipantIndexKey(resolved_index_prefix, txn_id, row.value);
        if (row.value.len == 0 or !std.mem.eql(u8, row.key, &expected)) return error.InvalidTxnRecord;
        try names.put(scratch, try scratch.dupe(u8, row.value), {});
        count += 1;
        entry = try cursor.next();
    }
    const marker_key = makeSidecarKey(participant_index_prefix, txn_id);
    const indexed = try ParticipantIndex.decode(try read.get(&marker_key));
    if (count != indexed.resolved or names.count() > indexed.enlisted) return error.InvalidTxnRecord;
    const result = try alloc.alloc([]u8, names.count());
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |name| alloc.free(name);
        alloc.free(result);
    }
    var it = names.keyIterator();
    for (result) |*name| {
        name.* = try alloc.dupe(u8, it.next().?.*);
        initialized += 1;
    }
    std.mem.sort([]u8, result, {}, struct {
        fn less(_: void, a: []u8, b: []u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.less);
    return result;
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

pub fn makeTransactionReplicationBatchOutboxKey(txn_id: TxnId) [hot_standby_batch_outbox_prefix.len + 16]u8 {
    return makeSidecarKey(hot_standby_batch_outbox_prefix, txn_id);
}

pub fn makeTransactionReplicationReplayOutboxKey(txn_id: TxnId) [hot_standby_replay_outbox_prefix.len + 16]u8 {
    return makeSidecarKey(hot_standby_replay_outbox_prefix, txn_id);
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
    if (raw.len == 0 or raw[0] > @backingInt(TxnStatus.aborted)) return TxnError.InvalidTxnRecord;
    if (raw.len == txn_record_v8_size) {
        var record = try decodeRecord(raw[0..txn_record_v7_size]);
        record.preparation_binding = prepared_effects.Binding.decode(raw[txn_record_v7_size..]) catch return error.InvalidTxnRecord;
        return record;
    }
    if (raw.len == txn_record_v7_size) {
        const mode: prepared_effects.Mode = switch (raw[53]) {
            1 => .deferred,
            2 => .synchronous,
            else => return TxnError.InvalidTxnRecord,
        };
        var record = try decodeRecord(raw[0..txn_record_v6_size]);
        record.preparation_requirement = .{ .mode = mode };
        return record;
    }
    if (raw.len == txn_record_v6_size) {
        if (raw[49] > 1 or raw[50] > 1 or raw[51] > 1 or raw[52] > 1) return TxnError.InvalidTxnRecord;
        return .{
            .status = @fromBackingInt(@intCast(raw[0])),
            .begin_timestamp = std.mem.readInt(u64, raw[1..9], .little),
            .commit_version = std.mem.readInt(u64, raw[9..17], .little),
            .created_at = std.mem.readInt(u64, raw[17..25], .little),
            .finalized_at = std.mem.readInt(u64, raw[25..33], .little),
            .intent_revision = std.mem.readInt(u64, raw[33..41], .little),
            .replay_sequence = std.mem.readInt(u64, raw[41..49], .little),
            .prepared = raw[49] == 1,
            .coordinator = raw[50] == 1,
            .retain_terminal = raw[51] == 1,
            .intents_resolved = raw[52] == 1,
            .intents_resolved_known = true,
        };
    }
    if (raw.len == txn_record_v5_size) {
        if (raw[49] > 1 or raw[50] > 1 or raw[51] > 1) return TxnError.InvalidTxnRecord;
        return .{
            .status = @fromBackingInt(@intCast(raw[0])),
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
            .status = @fromBackingInt(@intCast(raw[0])),
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
            .status = @fromBackingInt(@intCast(raw[0])),
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
            .status = @fromBackingInt(@intCast(raw[0])),
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
            .status = @fromBackingInt(@intCast(raw[0])),
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
        const status: TxnStatus = @fromBackingInt(@intCast(raw[0]));
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
    return participantListsEqualWithAdmission(a, b, null) catch unreachable;
}

pub fn participantListsEqualWithAdmission(a: []const []const u8, b: []const []const u8, control: ?read_control.Port) !bool {
    if (control) |owner| try owner.checkpoint();
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (control) |owner| try owner.checkpoint();
        if (left.len != right.len) return false;
        var offset: usize = 0;
        while (offset < left.len) {
            if (control) |owner| try owner.checkpoint();
            const end = offset + @min(left.len - offset, 4096);
            if (!std.mem.eql(u8, left[offset..end], right[offset..end])) return false;
            offset = end;
        }
    }
    if (control) |owner| try owner.checkpoint();
    return true;
}

fn encodeParticipantList(alloc: Allocator, participants: []const []const u8) ![]u8 {
    return encodeParticipantListWithAdmission(alloc, participants, null);
}

pub fn encodeParticipantListWithAdmission(alloc: Allocator, participants: []const []const u8, control: ?read_control.Port) ![]u8 {
    if (control) |owner| try owner.checkpoint();
    if (participants.len > std.math.maxInt(u32)) return TxnError.InvalidTxnRecord;
    var total: usize = 4;
    for (participants) |participant| {
        if (control) |owner| try owner.checkpoint();
        if (participant.len > std.math.maxInt(u32)) return TxnError.InvalidTxnRecord;
        total = std.math.add(usize, total, 4) catch return error.OutOfMemory;
        total = std.math.add(usize, total, participant.len) catch return error.OutOfMemory;
    }
    const buf = try alloc.alloc(u8, total);
    errdefer alloc.free(buf);
    std.mem.writeInt(u32, buf[0..4], @intCast(participants.len), .little);
    var offset: usize = 4;
    for (participants) |participant| {
        if (control) |owner| try owner.checkpoint();
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(participant.len), .little);
        offset += 4;
        var copied: usize = 0;
        while (copied < participant.len) {
            if (control) |owner| try owner.checkpoint();
            const end = copied + @min(participant.len - copied, 4096);
            @memcpy(buf[offset + copied .. offset + end], participant[copied..end]);
            copied = end;
        }
        offset += participant.len;
    }
    if (control) |owner| try owner.checkpoint();
    return buf;
}

fn decodeParticipantList(alloc: Allocator, raw: []const u8) ![][]u8 {
    return decodeParticipantListWithAdmission(alloc, raw, null);
}

pub fn decodeParticipantListWithAdmission(alloc: Allocator, raw: []const u8, control: ?read_control.Port) ![][]u8 {
    if (control) |owner| try owner.checkpoint();
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
        if (control) |owner| try owner.checkpoint();
        if (offset + 4 > raw.len) return TxnError.InvalidTxnRecord;
        var len_buf: [4]u8 = undefined;
        @memcpy(&len_buf, raw[offset .. offset + 4]);
        const len = std.mem.readInt(u32, &len_buf, .little);
        offset += 4;
        if (offset + len > raw.len) return TxnError.InvalidTxnRecord;
        result[i] = try backend_scan.dupeWithAdmission(alloc, raw[offset .. offset + len], control);
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
    return try alloc.dupeSentinel(u8, path, 0);
}

// ============================================================================
// Tests
// ============================================================================

test "prepared transaction effects required begin survives restart before and after sealing" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "required-prepare-reopen");
    defer a.free(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(0xb1);
    const catalogs: [32]u8 = @splat(55);
    const receipt_key = "\x00\x00__metadata__:required-begin-receipt";
    {
        var backend = try lsm_backend.Backend.open(a, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.initTransactionRequiringPreparedEffects(id, 10, 11, &.{}, false, false, .{ .mode = .synchronous }, .{ .writes = &.{.{ .key = receipt_key, .value = "same-batch" }} }, null);
        try std.testing.expectError(error.PreparedEffectsRequired, manager.resolveIntents(id, .committed, 100));
        try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(id));
        try manager.initTransaction(id, 10);
        try std.testing.expectEqual(prepared_effects.Mode.synchronous, (try manager.loadPreparationRequirement(id, null)).?.mode);
        try manager.initTransactionRequiringPreparedEffects(id, 10, 11, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
        try std.testing.expectError(error.PreparationContractConflict, manager.initTransactionRequiringPreparedEffects(id, 10, 11, &.{}, false, false, .{ .mode = .deferred }, .{}, null));
        try manager.writeIntents(id, &.{.{ .key = "row", .value = "first" }}, &.{});
        try manager.writeIntents(id, &.{.{ .key = "other", .value = null }}, &.{});
        const receipt = try store.get(a, receipt_key);
        defer a.free(receipt);
        try std.testing.expectEqualStrings("same-batch", receipt);
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 2), (try publication_backlog.load(&read)).?.records);
    }
    {
        var backend = try lsm_backend.Backend.open(a, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try std.testing.expectError(error.PreparedEffectsRequired, manager.resolveIntents(id, .committed, 100));
        try manager.writeIntents(id, &.{.{ .key = "row", .value = "second" }}, &.{});
        const contract = try manager.preparedEffectsContract(id, catalogs, .synchronous, null);
        const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
        defer a.free(frame);
        try std.testing.expectError(error.PreparationContractConflict, manager.sealPreparedEffects(id, frame, catalogs, .deferred, null));
        try manager.sealPreparedEffects(id, frame, catalogs, .synchronous, null);
        try manager.initTransactionRequiringPreparedEffects(id, 10, 11, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 3), (try publication_backlog.load(&read)).?.records);
    }
    {
        var backend = try lsm_backend.Backend.open(a, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try std.testing.expectEqual(prepared_effects.Mode.synchronous, (try manager.loadPreparationRequirement(id, null)).?.mode);
        var plan = (try manager.loadPreparedEffects(a, id, null)).?;
        defer plan.deinit();
        _ = try manager.resolvePreparedIntents(id, 100, &plan, .{}, null);
        try std.testing.expect(!try manager.hasPreparationRequirement(id));
        try std.testing.expect(!try manager.hasPreparedEffects(id));
        const row = try getVisibleDocRuntime(&manager.store, a, "row");
        defer a.free(row);
        try std.testing.expectEqualStrings("second", row);
        try std.testing.expectError(TxnError.DecisionConflict, manager.initTransactionRequiringPreparedEffects(id, 10, 11, &.{}, false, false, .{ .mode = .synchronous }, .{}, null));
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 0), (try publication_backlog.load(&read)).?.records);
        const key = makePreparationRequirementKey(id);
        try std.testing.expect(!@import("portable_backup.zig").isPortableMetadataKey(&key));
    }
}

test "prepared transaction effects required begin refuses legacy upgrades and corrupt modes" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const legacy: TxnId = @splat(0xb2);
    const required: TxnId = @splat(0xb3);
    const orphan: TxnId = @splat(0xb4);
    const reserved_key = makePreparationRequirementKey(required);
    const reserved_frame = makePreparedEffectsKey(required);
    const forged = (prepared_effects.Requirement{ .mode = .deferred }).encode();
    try std.testing.expectError(error.InvalidPreparationRequirement, manager.initTransactionRequiringPreparedEffects(required, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{ .writes = &.{.{ .key = &reserved_key, .value = &forged }} }, null));
    try std.testing.expectError(error.InvalidPreparationRequirement, manager.initTransactionRequiringPreparedEffects(required, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{ .deletes = &.{&reserved_key} }, null));
    try std.testing.expectError(error.InvalidPreparationRequirement, manager.initTransactionRequiringPreparedEffects(required, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{ .writes = &.{.{ .key = &reserved_frame, .value = "forged" }} }, null));
    try std.testing.expectError(TxnError.TxnNotFound, manager.getTransactionStatus(required));
    try manager.initTransaction(legacy, 1);
    try std.testing.expectError(error.PreparationContractConflict, manager.initTransactionRequiringPreparedEffects(legacy, 1, 1, &.{}, false, false, .{ .mode = .deferred }, .{}, null));
    try manager.writeIntents(legacy, &.{.{ .key = "legacy", .value = "unchanged" }}, &.{});
    try manager.resolveIntents(legacy, .committed, 10);
    const legacy_requirement = makePreparationRequirementKey(legacy);
    try manager.applyBatch(&.{.{ .key = &legacy_requirement, .value = &forged }}, &.{}, null);
    try std.testing.expectError(error.InvalidTxnRecord, manager.resolveIntents(legacy, .committed, 10));
    try std.testing.expectError(TxnError.DecisionConflict, manager.resolveIntents(legacy, .aborted, 10));
    try std.testing.expect(!try manager.cleanupTransactionMetadataIfEligibleExtraBatch(legacy, 100, 100, .{}));
    try std.testing.expect(try manager.hasPreparationRequirement(legacy));
    try manager.initTransactionRequiringPreparedEffects(required, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
    const catalogs: [32]u8 = @splat(56);
    try manager.writeIntents(required, &.{.{ .key = "required", .value = "uncommitted" }}, &.{});
    const contract = try manager.preparedEffectsContract(required, catalogs, .synchronous, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try manager.sealPreparedEffects(required, frame, catalogs, .synchronous, null);
    var plan = (try manager.loadPreparedEffects(a, required, null)).?;
    defer plan.deinit();
    const key = makePreparationRequirementKey(required);
    var raw = (prepared_effects.Requirement{ .mode = .deferred }).encode();
    try manager.applyBatch(&.{.{ .key = &key, .value = &raw }}, &.{}, null);
    try std.testing.expectError(error.PreparationContractConflict, manager.loadPreparedEffects(a, required, null));
    try std.testing.expectError(error.PreparationContractConflict, manager.resolvePreparedIntents(required, 10, &plan, .{}, null));
    raw[39] ^= 1;
    try manager.applyBatch(&.{.{ .key = &key, .value = &raw }}, &.{}, null);
    try std.testing.expectError(error.InvalidPreparationRequirement, manager.resolveIntents(required, .committed, 10));
    try manager.resolveIntents(required, .aborted, 10);
    try std.testing.expect(!try manager.hasPreparationRequirement(required));
    try std.testing.expect(!try manager.hasPreparedEffects(required));
    // An aborted terminal record can retire an opaque leftover requirement.
    try manager.applyBatch(&.{.{ .key = &key, .value = &raw }}, &.{}, null);
    try manager.resolveIntents(required, .aborted, 10);
    try std.testing.expect(!try manager.hasPreparationRequirement(required));
    const orphan_key = makePreparationRequirementKey(orphan);
    const valid = (prepared_effects.Requirement{ .mode = .deferred }).encode();
    try manager.applyBatch(&.{.{ .key = &orphan_key, .value = &valid }}, &.{}, null);
    try std.testing.expectError(error.InvalidTxnRecord, manager.initTransactionRequiringPreparedEffects(orphan, 1, 1, &.{}, false, false, .{ .mode = .deferred }, .{}, null));
    try std.testing.expectError(TxnError.TxnNotFound, manager.getTransactionStatus(orphan));
}

test "prepared transaction effects required begin reserves before publication and retries at capacity" {
    const a = std.testing.allocator;
    var backend = lsm_backend.Backend.init(a, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const id: TxnId = @splat(0xb5);
    const refused: TxnId = @splat(0xb6);
    try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, false, .{ .mode = .deferred }, .{}, null);
    const prefix = @import("db/durable_outbox.zig").replication_outbox_v2_prefix;
    var filler: [prefix.len + 8]u8 = undefined;
    @memcpy(filler[0..prefix.len], prefix);
    var fill = try store.beginWriteBatch();
    errdefer fill.abort();
    for (0..publication_backlog.default_max_records - 1) |i| {
        std.mem.writeInt(u64, filler[prefix.len..][0..8], i, .big);
        try fill.put(&filler, "x");
    }
    try fill.commit();
    try std.testing.expectError(error.Backpressured, manager.initTransactionRequiringPreparedEffects(refused, 1, 1, &.{}, false, false, .{ .mode = .deferred }, .{ .writes = &.{.{ .key = "receipt-refused", .value = "absent" }} }, null));
    try std.testing.expectError(TxnError.TxnNotFound, manager.getTransactionStatus(refused));
    try std.testing.expect(!try manager.hasPreparationRequirement(refused));
    try std.testing.expectError(error.NotFound, store.get(a, "receipt-refused"));
    try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, false, .{ .mode = .deferred }, .{}, null);
    try std.testing.expectError(error.Backpressured, manager.writeIntents(id, &.{.{ .key = "row", .value = "first" }}, &.{}));
    try std.testing.expect(!(try manager.loadTransactionRecord(id)).prepared);
    try store.delete(&filler);
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "first" }}, &.{});
    const catalogs: [32]u8 = @splat(57);
    var contract = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try std.testing.expectError(error.Backpressured, manager.sealPreparedEffects(id, frame, catalogs, .deferred, null));
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "second" }}, &.{});
    std.mem.writeInt(u64, filler[prefix.len..][0..8], 0, .big);
    try store.delete(&filler);
    contract = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
    const updated = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(updated);
    try manager.sealPreparedEffects(id, updated, catalogs, .deferred, null);
    try manager.resolveIntents(id, .aborted, 10);
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(publication_backlog.default_max_records - 3, (try publication_backlog.load(&read)).?.records);
}

test "prepared transaction effects required begin releases every failed allocation atomically" {
    const Case = struct {
        fn run(failing: Allocator) !void {
            const a = std.testing.allocator;
            var backend = mem_backend.Backend.init(a, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(a, .{});
            defer runtime.deinit();
            var manager = try TxnManager.init(a, &runtime);
            defer manager.deinit();
            const id: TxnId = @splat(0xb7);
            manager.alloc = failing;
            const result = manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{ "one", "two" }, true, true, .{ .mode = .synchronous }, .{ .writes = &.{.{ .key = "required-receipt", .value = "atomic" }} }, null);
            manager.alloc = a;
            if (result) |_| {
                try std.testing.expect(try manager.hasPreparationRequirement(id));
                try std.testing.expectEqual(prepared_effects.Mode.synchronous, (try manager.getPreparationDecision(id, null)).requirement.?.mode);
                try manager.resolveIntents(id, .aborted, 10);
            } else |err| {
                try std.testing.expectError(TxnError.TxnNotFound, manager.getTransactionStatus(id));
                try std.testing.expect(!try manager.hasPreparationRequirement(id));
                try std.testing.expectError(error.NotFound, manager.getAlloc(a, "required-receipt"));
                return err;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prepared transaction effects required begin original rejections never create a legacy record" {
    const Control = struct {
        checks: usize = 0,
        reject: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject) return self.cause;
        }
    };
    const Case = struct {
        fn run(c: *Control) !void {
            const a = std.testing.allocator;
            var backend = mem_backend.Backend.init(a, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(a, .{});
            defer runtime.deinit();
            var manager = try TxnManager.init(a, &runtime);
            defer manager.deinit();
            const id: TxnId = @splat(0xb8);
            const payload: read_control.Payload = .{ .ptr = c, .vtable = &.{ .checkpoint = Control.check } };
            const port = read_control.Port.init(&payload);
            const result = manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{}, port);
            if (result) |_| {
                try manager.resolveIntents(id, .aborted, 10);
            } else |err| {
                try std.testing.expectError(TxnError.TxnNotFound, manager.getTransactionStatus(id));
                try std.testing.expect(!try manager.hasPreparationRequirement(id));
                return err;
            }
        }
    };
    var baseline: Control = .{};
    try Case.run(&baseline);
    try std.testing.expect(baseline.checks > 8);
    for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.MemoryLimitExceeded }) |cause| {
        for (1..baseline.checks + 1) |reject| {
            var stopped: Control = .{ .reject = reject, .cause = cause };
            try std.testing.expectError(cause, Case.run(&stopped));
            try std.testing.expectEqual(reject, stopped.checks);
        }
    }
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    var stopped: Control = .{ .reject = 1 };
    const payload: read_control.Payload = .{ .ptr = &stopped, .vtable = &.{ .checkpoint = Control.check } };
    try std.testing.expectError(error.NotFound, manager.loadPreparationRequirement(@splat(0xb9), read_control.Port.init(&payload)));
}

test "prepared transaction effects seal survives native reopen and consumes once" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "prepared-effects-seal");
    defer a.free(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(91);
    const catalogs: [32]u8 = @splat(17);
    const effect_key = "\x00\x00__metadata__:prepared-test-effect";
    var revision: u64 = 0;
    {
        var backend = try lsm_backend.Backend.open(a, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.initTransaction(id, 10);
        try manager.writeIntents(id, &.{.{ .key = "row", .value = "first" }}, &.{});
        try manager.writeIntents(id, &.{ .{ .key = "row", .value = "second" }, .{ .key = "deleted", .value = null } }, &.{});
        const contract = try manager.preparedEffectsContract(id, catalogs, .synchronous, null);
        revision = contract.intent_revision;
        const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{ .artifact_writes = @constCast(&[_]@import("db/types.zig").BatchWrite{.{ .key = effect_key, .value = "accepted" }}) } }, null);
        defer a.free(frame);
        var before_probe = try store.beginReadTxn();
        const before = (try publication_backlog.load(&before_probe)).?;
        before_probe.abort();
        try manager.sealPreparedEffects(id, frame, catalogs, .synchronous, null);
        var after_probe = try store.beginReadTxn();
        const after = (try publication_backlog.load(&after_probe)).?;
        after_probe.abort();
        try std.testing.expectEqual(before.records + 1, after.records);
        try std.testing.expectEqual(before.bytes + makePreparedEffectsKey(id).len + frame.len, after.bytes);
        try manager.sealPreparedEffects(id, frame, catalogs, .synchronous, null);
        try std.testing.expectEqual(revision, (try manager.loadTransactionRecord(id)).intent_revision);
        try std.testing.expectError(error.PreparedEffectsSealed, manager.writeIntents(id, &.{.{ .key = "row", .value = "changed" }}, &.{}));
        try std.testing.expectError(error.PreparedEffectsRequired, manager.resolveIntents(id, .committed, 100));
        const different = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
        defer a.free(different);
        try std.testing.expectError(error.PreparedEffectsConflict, manager.sealPreparedEffects(id, different, catalogs, .synchronous, null));
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualDeep(after, (try publication_backlog.load(&read)).?);
    }
    {
        var backend = try lsm_backend.Backend.open(a, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        var plan = (try manager.loadPreparedEffects(a, id, null)).?;
        defer plan.deinit();
        try std.testing.expectEqual(revision, plan.decoded.plan.contract.intent_revision);
        try std.testing.expectEqual(prepared_effects.Mode.synchronous, plan.decoded.plan.contract.mode);
        try std.testing.expectError(error.PreparedEffectsRequired, manager.resolveIntents(id, .committed, 100));
        const effect = plan.decoded.plan.effects.artifact_writes[0];
        const batch: ResolutionExtraBatch = .{ .writes = &.{.{ .key = effect.key, .value = effect.value }} };
        try std.testing.expect((try manager.resolvePreparedIntents(id, 100, &plan, batch, null)).applied);
        try std.testing.expect(!try manager.hasPreparedEffects(id));
        try std.testing.expect((try manager.loadPreparedEffects(a, id, null)) == null);
        const visible = try getVisibleDocRuntime(&manager.store, a, "row");
        defer a.free(visible);
        try std.testing.expectEqualStrings("second", visible);
        const output = try store.get(a, effect_key);
        defer a.free(output);
        try std.testing.expectEqualStrings("accepted", output);
        try store.put(effect_key, "newer");
        try std.testing.expect(!(try manager.resolvePreparedIntents(id, 100, &plan, batch, null)).applied);
        const newer = try store.get(a, effect_key);
        defer a.free(newer);
        try std.testing.expectEqualStrings("newer", newer);
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 0), (try publication_backlog.load(&read)).?.records);
    }
}

test "prepared transaction effects refuse stale inputs mode schema and missing seals" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(92);
    const catalogs: [32]u8 = @splat(18);
    try manager.initTransaction(id, 10);
    try manager.writeIntentsExtraBatch(id, &.{.{ .key = "row", .value = "{}", .prepared_row = "first-row" }}, &.{}, .{ .schema_binding = .{ .version = 19 } });
    const contract = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
    const old = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(old);
    try manager.writeIntentsExtraBatch(id, &.{.{ .key = "row", .value = "{}", .prepared_row = "second-row" }}, &.{}, .{ .schema_binding = .{ .version = 19 } });
    try std.testing.expectError(error.PreparedEffectsMismatch, manager.sealPreparedEffects(id, old, catalogs, .deferred, null));
    const current = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
    try std.testing.expect(!std.mem.eql(u8, &contract.intent_digest, &current.intent_digest));
    var wrong = current;
    wrong.schema_version = 20;
    const wrong_frame = try prepared_effects.encode(a, .{ .contract = wrong, .effects = .{} }, null);
    defer a.free(wrong_frame);
    try std.testing.expectError(error.PreparedEffectsMismatch, manager.sealPreparedEffects(id, wrong_frame, catalogs, .deferred, null));
    const frame = try prepared_effects.encode(a, .{ .contract = current, .effects = .{} }, null);
    defer a.free(frame);
    try std.testing.expectError(error.PreparedEffectsMismatch, manager.sealPreparedEffects(id, frame, catalogs, .synchronous, null));
    try std.testing.expectError(error.PreparedEffectsMismatch, manager.sealPreparedEffects(id, frame, @splat(99), .deferred, null));
    try std.testing.expect(!try manager.hasPreparedEffects(id));
    try manager.sealPreparedEffects(id, frame, catalogs, .deferred, null);
    var plan = (try manager.loadPreparedEffects(a, id, null)).?;
    defer plan.deinit();
    const key = makePreparedEffectsKey(id);
    try manager.applyBatch(&.{}, &.{&key}, null);
    try std.testing.expectError(error.PreparedEffectsRequired, manager.resolvePreparedIntents(id, 100, &plan, .{}, null));
    try manager.sealPreparedEffects(id, frame, catalogs, .deferred, null);
    // Abort retires opaque bytes; corrupt plans cannot make abort depend on a
    // successful decoder or synchronous producer.
    const bad = try a.dupe(u8, frame);
    defer a.free(bad);
    bad[bad.len - 1] ^= 1;
    try manager.applyBatch(&.{.{ .key = &key, .value = bad }}, &.{}, null);
    try std.testing.expectError(error.InvalidPreparedEffects, manager.loadPreparedEffects(a, id, null));
    try manager.resolveIntents(id, .aborted, 100);
    try std.testing.expect(!try manager.hasPreparedEffects(id));
}

test "prepared transaction effects admission refuses sealing before durable growth" {
    const a = std.testing.allocator;
    var backend = lsm_backend.Backend.init(a, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const id: TxnId = @splat(93);
    const catalogs: [32]u8 = @splat(19);
    try manager.initTransaction(id, 1);
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
    var contract = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    const prefix = @import("db/durable_outbox.zig").replication_outbox_v2_prefix;
    var filler: [prefix.len + 8]u8 = undefined;
    @memcpy(filler[0..prefix.len], prefix);
    var fill = try store.beginWriteBatch();
    errdefer fill.abort();
    for (0..publication_backlog.default_max_records - 1) |i| {
        std.mem.writeInt(u64, filler[prefix.len..][0..8], i, .big);
        try fill.put(&filler, "x");
    }
    try fill.commit();
    try std.testing.expectError(error.Backpressured, manager.sealPreparedEffects(id, frame, catalogs, .deferred, null));
    try std.testing.expect(!try manager.hasPreparedEffects(id));
    try std.testing.expectEqual(contract.intent_revision, (try manager.loadTransactionRecord(id)).intent_revision);
    // Refusal did not freeze the existing prepare or consume a record credit.
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "updated" }}, &.{});
    try store.delete(&filler);
    contract = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
    const updated = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(updated);
    try manager.sealPreparedEffects(id, updated, catalogs, .deferred, null);
    try manager.sealPreparedEffects(id, updated, catalogs, .deferred, null);
    try manager.resolveIntents(id, .aborted, 10);
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(publication_backlog.default_max_records - 2, (try publication_backlog.load(&read)).?.records);
}

test "prepared transaction effects seal and load release every allocation failure" {
    const Case = struct {
        fn run(failing: Allocator) !void {
            const a = std.testing.allocator;
            var backend = mem_backend.Backend.init(a, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(a, .{});
            defer runtime.deinit();
            var manager = try TxnManager.init(a, &runtime);
            defer manager.deinit();
            const id: TxnId = @splat(94);
            const catalogs: [32]u8 = @splat(20);
            try manager.initTransaction(id, 1);
            try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
            const contract = try manager.preparedEffectsContract(id, catalogs, .synchronous, null);
            const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{ .artifact_delete_keys = &.{"old-artifact"} } }, null);
            defer a.free(frame);
            manager.alloc = failing;
            const sealed = manager.sealPreparedEffects(id, frame, catalogs, .synchronous, null);
            manager.alloc = a;
            if (sealed) |_| {} else |err| {
                try std.testing.expect(!try manager.hasPreparedEffects(id));
                try std.testing.expectEqual(contract.intent_revision, (try manager.loadTransactionRecord(id)).intent_revision);
                return err;
            }
            manager.alloc = failing;
            const loaded = manager.loadPreparedEffects(failing, id, null);
            manager.alloc = a;
            if (loaded) |value| {
                var owned = value.?;
                owned.deinit();
            } else |err| {
                try std.testing.expect(try manager.hasPreparedEffects(id));
                try manager.resolveIntents(id, .aborted, 10);
                return err;
            }
            try manager.resolveIntents(id, .aborted, 10);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "transaction batch publication reserves before vote and delivers at quota across restart" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-publication-credit");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(81);
    const refused_id: TxnId = @splat(82);
    const credit_key = publication_backlog.transactionCreditKey(id);
    const batch_key = makeTransactionReplicationBatchOutboxKey(id);
    const effects = @import("db/replication_effects.zig");
    const payload = try effects.encodeBatchMutationRequestAlloc(alloc, .{ .writes = &.{.{ .key = "row", .value = "{}" }}, .timestamp_ns = 10 });
    defer alloc.free(payload);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.initTransaction(id, 1);
        try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
        const credit = try store.get(alloc, &credit_key);
        defer alloc.free(credit);
        try std.testing.expect(payload.len <= std.mem.readInt(u64, credit[0..8], .little));
        var fill = try store.beginWriteBatch();
        var fill_open = true;
        defer if (fill_open) fill.abort();
        const prefix = @import("db/durable_outbox.zig").replication_outbox_v2_prefix;
        var filler: [prefix.len + 8]u8 = undefined;
        @memcpy(filler[0..prefix.len], prefix);
        for (0..publication_backlog.default_max_records - 1) |i| {
            std.mem.writeInt(u64, filler[prefix.len..][0..8], i, .big);
            try fill.put(&filler, "x");
        }
        try fill.commit();
        fill_open = false;
        try manager.initTransaction(refused_id, 2);
        try std.testing.expectError(error.Backpressured, manager.writeIntents(refused_id, &.{.{ .key = "refused", .value = "{}" }}, &.{}));
        try std.testing.expect(!(try manager.loadTransactionRecord(refused_id)).prepared);
        try std.testing.expect(!(try manager.hasIntents(refused_id)));
        // Replacing an existing prepare reuses its record credit at capacity.
        try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
        _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 10, .{ .writes = &.{.{ .key = &batch_key, .value = payload }} });
        try std.testing.expect(!(try manager.hasIntents(id)));
        const held = try store.get(alloc, &credit_key);
        defer alloc.free(held);
        try std.testing.expectEqualSlices(u8, credit, held);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        var pending = try manager.loadReplicationOutbox(alloc, id);
        defer pending.deinit(alloc);
        try std.testing.expectEqualSlices(u8, payload, pending.batch_payload.?);
        try std.testing.expectError(error.Backpressured, manager.writeIntents(refused_id, &.{.{ .key = "refused", .value = "{}" }}, &.{}));
        try manager.clearReplicationOutbox(id, .batch);
        try manager.clearReplicationOutbox(id, .batch);
        try std.testing.expectError(error.NotFound, store.get(alloc, &credit_key));
        try manager.writeIntents(refused_id, &.{.{ .key = "refused", .value = "{}" }}, &.{});
        try manager.resolveIntents(refused_id, .aborted, 11);
        const aborted_credit = publication_backlog.transactionCreditKey(refused_id);
        try std.testing.expectError(error.NotFound, store.get(alloc, &aborted_credit));
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(publication_backlog.default_max_records - 1, (try publication_backlog.load(&read)).?.records);
    }
}

test "transaction batch publication fences legacy prepares without blocking their committed outbox" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const legacy: TxnId = @splat(83);
    const next: TxnId = @splat(84);
    try manager.initTransaction(legacy, 1);
    try manager.initTransaction(next, 2);
    var record = try manager.loadTransactionRecord(legacy);
    record.prepared = true;
    record.intent_revision = 1;
    const raw_record = try manager.encodeRecord(record);
    defer alloc.free(raw_record);
    const record_key = makeRecordKey(legacy);
    try store.put(&record_key, raw_record);
    const intent_key = try manager.makeIntentKey(legacy, "legacy");
    defer alloc.free(intent_key);
    try store.put(intent_key, "\x00{}");
    try std.testing.expectError(error.Backpressured, manager.writeIntents(next, &.{.{ .key = "new", .value = "{}" }}, &.{}));
    const outbox_key = makeTransactionReplicationBatchOutboxKey(legacy);
    _ = try manager.resolveIntentsWithExtraBatch(legacy, .committed, 3, .{ .writes = &.{.{ .key = &outbox_key, .value = "legacy HA payload" }} });
    try std.testing.expectError(error.Backpressured, manager.writeIntents(next, &.{.{ .key = "new", .value = "{}" }}, &.{}));
    try manager.clearReplicationOutbox(legacy, .batch);
    try manager.writeIntents(next, &.{.{ .key = "new", .value = "{}" }}, &.{});
    try manager.resolveIntents(next, .aborted, 4);
}

test "transaction batch publication budget covers actual escaped and binary request envelopes" {
    const alloc = std.testing.allocator;
    const effects = @import("db/replication_effects.zig");
    const fixed = try std.json.Stringify.valueAlloc(alloc, effects.BatchMutationPayload{
        .schema_version = 21,
        .ordinary_raft_entry = .{ .term = std.math.maxInt(u64), .index = std.math.maxInt(u64) },
        .request = .{ .timestamp_ns = std.math.maxInt(u64), .restore_staging_scope = @splat(255), .sync_level = .full_index },
    }, .{});
    defer alloc.free(fixed);
    try std.testing.expect(fixed.len < 64 * 1024);
    const pattern = [_]u8{ 0, 1, 8, 9, 10, 13, 31, '"', '\\' };
    var bytes: [576]u8 = undefined;
    for (&bytes, 0..) |*byte, i| byte.* = pattern[i % pattern.len];
    for ([_][]const u8{ "row:\x00\"\\\n", "\x00\x00__metadata__:binary\xff\x00" }) |key| {
        const intent: WriteIntent = .{ .key = key, .value = &bytes };
        const payload = try effects.encodeBatchMutationRequestAlloc(alloc, .{ .writes = &.{.{ .key = key, .value = &bytes }}, .deletes = &.{key}, .timestamp_ns = std.math.maxInt(u64) });
        defer alloc.free(payload);
        const cost = try intentAdmissionBytes(intent);
        try std.testing.expect(payload.len <= try publication_backlog.transactionBatchBudget(cost * 2));
    }
    try std.testing.expectError(error.TransactionTooLarge, publication_backlog.transactionBatchBudget(std.math.maxInt(u64)));
}

test "retained transaction reservations survive LSM reopen and guarantee committed resolution at quota" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-retained-reserved");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(73);
    const ns: retained_effects.Namespace = @splat(1);
    // A near-frame-limit transaction remains admissible: reserving a blanket
    // six-times JSON allowance would wrongly reject this before preparation.
    const payload = try alloc.alloc(u8, retained_effects.max_frame_bytes - 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');
    const physical = try internal_keys.documentKeyAlloc(alloc, "reserved");
    defer alloc.free(physical);
    const filler_key = try internal_keys.documentKeyAlloc(alloc, "filler");
    defer alloc.free(filler_key);
    const integrity = @import("db/relational_integrity_contract.zig");
    const address = try integrity.Address.init(@splat(3), "parent");
    const claim_key = address.claimKey();
    const claim_value = try (integrity.Claim{ .tuple = "parent", .parent_table = "parents", .parent_key = "reserved", .schema_version = 1 }).encode(alloc, address);
    defer alloc.free(claim_value);
    const claim_intent: WriteIntent = .{ .key = &claim_key, .value = claim_value };
    var expected_reserved: u64 = 0;
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &ns);
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            _ = try retained_effects.admit(&txn, ns, 1, @splat(1), retained_effects.max_frame_bytes);
            try txn.commit();
        }
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.initTransaction(id, 1);
        const intent: WriteIntent = .{ .key = "reserved", .value = payload };
        try manager.writeIntents(id, &.{ intent, claim_intent }, &.{});
        expected_reserved = (try manager.loadIntentAdmission(alloc, id)).?.reservation();
        try std.testing.expectEqual(@as(u64, payload.len + physical.len + 16 + 48 + claim_key.len + claim_value.len + 16), expected_reserved);
        // Repeated/duplicate-key prepares replace their credits, not add them.
        try manager.writeIntents(id, &.{ intent, claim_intent, .{ .key = "reserved", .value = "smaller" }, intent }, &.{});
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(expected_reserved, (try retained_effects.loadReservations(&read)).?.bytes);
        }
        const filler = try alloc.alloc(u8, @intCast(retained_effects.max_frame_bytes - expected_reserved - 48 - 16 - filler_key.len));
        defer alloc.free(filler);
        @memset(filler, 'f');
        try store.put(filler_key, filler);
        try std.testing.expectError(error.RetainedEffectsFull, store.put(filler_key, "cannot steal committed credits"));
        const rejected: TxnId = @splat(74);
        try manager.initTransaction(rejected, 2);
        try std.testing.expectError(error.RetainedEffectsFull, manager.writeIntents(rejected, &.{.{ .key = "another", .value = "x" }}, &.{}));
        try manager.checkOrdinaryWriteConflict("another");
        try std.testing.expect(!(try manager.loadTransactionRecord(rejected)).prepared);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.resolveIntents(id, .committed, 1234);
        try manager.resolveIntents(id, .committed, 1234);
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualSlices(u8, payload, try read.get(physical));
        try std.testing.expectEqualSlices(u8, claim_value, try read.get(&claim_key));
        try std.testing.expectEqual(@as(u64, 0), (try retained_effects.loadReservations(&read)).?.bytes);
        const state = (try retained_effects.load(&read)).?;
        try std.testing.expectEqual(@as(u64, 2), state.latest);
        try std.testing.expectEqual(@as(u64, retained_effects.max_frame_bytes), state.retained_bytes);
        var reader = (try retained_effects.read(&read, ns, 1, @splat(1), 1)).?;
        const claim_effect = (try reader.next()).?;
        try std.testing.expect(claim_effect.isIntegrity());
        try std.testing.expectEqualSlices(u8, claim_value, claim_effect.value.?);
        try std.testing.expectEqual(@as(u64, 0), claim_effect.timestamp);
        const effect = (try reader.next()).?;
        try std.testing.expectEqual(@as(u64, 1234), effect.timestamp);
        try std.testing.expect(try reader.next() == null);
    }
}

test "workload accepted transactions preserve intents on node pressure and retry durably" {
    const alloc = std.testing.allocator;
    const ResourceManager = @import("resource_manager.zig").ResourceManager;
    var resources = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 }, .identity_allocator = alloc });
    defer resources.deinit(alloc);
    const path = try tempTestPath(alloc, "txn-node-pressure");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    const committed: TxnId = @splat(91);
    const aborted: TxnId = @splat(92);
    const committed_key = try internal_keys.documentKeyAlloc(alloc, "accepted");
    defer alloc.free(committed_key);
    const aborted_key = try internal_keys.documentKeyAlloc(alloc, "discarded");
    defer alloc.free(aborted_key);
    {
        // Release competing ownership only after backend shutdown has joined.
        var pressure: u64 = 0;
        defer if (pressure != 0) resources.releaseExternal(pressure);
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .resource_manager = &resources });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.initTransaction(committed, 1);
        try manager.writeIntents(committed, &.{.{ .key = "accepted", .value = "durable" }}, &.{});
        try manager.initTransaction(aborted, 2);
        try manager.writeIntents(aborted, &.{.{ .key = "discarded", .value = "never visible" }}, &.{});
        var committed_batch = try manager.collectIntentBatch(alloc, committed);
        defer committed_batch.deinit(alloc);
        var aborted_batch = try manager.collectIntentBatch(alloc, aborted);
        defer aborted_batch.deinit(alloc);
        const snapshot = resources.snapshot().memory;
        pressure = snapshot.hard_limit_bytes - snapshot.used_bytes;
        try std.testing.expect(resources.reserveExternal(pressure));
        try std.testing.expect(!resources.reserveExternal(1));
        // A captured intent list alone does not reserve downstream cursors or
        // publication memory. Preserve this failure as a regression until a
        // typed completion reservation spans the entire storage operation.
        try std.testing.expectError(error.ResourceBudgetExceeded, manager.resolveIntentsWithExtraBatch(committed, .committed, 100, .{ .captured_intents = committed_batch.owned_entries, .expected_intent_revision = committed_batch.revision }));
        try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(committed));
        resources.releaseExternal(pressure);
        pressure = 0;
        _ = try manager.resolveIntentsWithExtraBatch(committed, .committed, 100, .{ .captured_intents = committed_batch.owned_entries, .expected_intent_revision = committed_batch.revision });
        _ = try manager.resolveIntentsWithExtraBatch(aborted, .aborted, 101, .{ .captured_intents = aborted_batch.owned_entries, .expected_intent_revision = aborted_batch.revision });
        try std.testing.expectEqual(TxnStatus.committed, try manager.getTransactionStatus(committed));
        try std.testing.expectEqual(TxnStatus.aborted, try manager.getTransactionStatus(aborted));
        try std.testing.expectEqual(@as(u64, 0), resources.sliceStats(.lsm_wal_write_working_set).used_bytes);
        const after = resources.snapshot().memory;
        pressure = after.hard_limit_bytes - after.used_bytes;
        try std.testing.expect(resources.reserveExternal(pressure));
    }
    // Check persisted mutations and transaction decisions after all owners close.
    var backend = try lsm_backend.Backend.open(alloc, path, .{ .resource_manager = &resources });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualSlices(u8, "durable", try read.get(committed_key));
    try std.testing.expectError(error.NotFound, read.get(aborted_key));
    try std.testing.expectEqual(TxnStatus.committed, try manager.getTransactionStatus(committed));
    try std.testing.expectEqual(TxnStatus.aborted, try manager.getTransactionStatus(aborted));
}

test "workload accepted transactions recover durable decisions after pressured shutdown" {
    const alloc = std.testing.allocator;
    const ResourceManager = @import("resource_manager.zig").ResourceManager;
    var resources = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 }, .identity_allocator = alloc });
    defer resources.deinit(alloc);
    const path = try tempTestPath(alloc, "txn-decided-node-pressure");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    const committed: TxnId = @splat(93);
    const aborted: TxnId = @splat(94);
    const committed_key = try internal_keys.documentKeyAlloc(alloc, "accepted");
    defer alloc.free(committed_key);
    const aborted_key = try internal_keys.documentKeyAlloc(alloc, "discarded");
    defer alloc.free(aborted_key);
    {
        var pressure: u64 = 0;
        // Keep competing ownership held until the backend joins shutdown.
        defer if (pressure != 0) resources.releaseExternal(pressure);
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .resource_manager = &resources });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.initTransaction(committed, 1);
        try manager.writeIntents(committed, &.{.{ .key = "accepted", .value = "durable" }}, &.{});
        try manager.initTransaction(aborted, 2);
        try manager.writeIntents(aborted, &.{.{ .key = "discarded", .value = "never visible" }}, &.{});
        // Model a participant that has persisted an externally learned decision
        // but has not yet published or removed its intents. Preserve the real
        // intent revision and current record encoding for restart recovery.
        for ([_]TxnId{ committed, aborted }, [_]TxnStatus{ .committed, .aborted }) |id, status| {
            var record = try manager.loadTransactionRecord(id);
            try applyResolveDecision(&record, status, 100);
            record.retain_terminal = true;
            try manager.saveTransactionRecord(makeRecordKey(id), record);
        }
        var batch = try manager.collectIntentBatch(alloc, committed);
        defer batch.deinit(alloc);
        const snapshot = resources.snapshot().memory;
        pressure = snapshot.hard_limit_bytes - snapshot.used_bytes;
        try std.testing.expect(resources.reserveExternal(pressure));
        try std.testing.expect(!resources.reserveExternal(1));
        try std.testing.expectError(error.ResourceBudgetExceeded, manager.resolveIntentsWithExtraBatch(committed, .committed, 100, .{ .captured_intents = batch.owned_entries, .expected_intent_revision = batch.revision }));
        try std.testing.expectEqual(TxnStatus.committed, try manager.getTransactionStatus(committed));
        try std.testing.expectEqual(TxnStatus.aborted, try manager.getTransactionStatus(aborted));
        try std.testing.expect(!(try manager.loadTransactionRecord(committed)).intents_resolved);
        // Shutdown still has no resource grant for finalization. Its one
        // deferred-finalization error is intentional; additional errors fail.
        @import("antfly_test_error_logs").expectErrorLogs(1);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .resource_manager = &resources });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try std.testing.expectEqual(TxnStatus.committed, try manager.getTransactionStatus(committed));
        try std.testing.expectEqual(TxnStatus.aborted, try manager.getTransactionStatus(aborted));
        const recovered = try manager.recoverTransactions(0, 101);
        try std.testing.expectEqual(@as(u64, 2), recovered.resolved_finalized);
        try std.testing.expectEqual(@as(u64, 0), recovered.auto_aborted);
        try std.testing.expect(!try manager.hasAnyIntents(committed));
        try std.testing.expect(!try manager.hasAnyIntents(aborted));
        try std.testing.expectEqual(@as(u64, 0), (try manager.recoverTransactions(0, 102)).resolved_finalized);
        try std.testing.expectEqual(@as(u64, 0), resources.sliceStats(.lsm_wal_write_working_set).used_bytes);
    }
    // A second reopen proves recovery durably published the chosen outcomes.
    var backend = try lsm_backend.Backend.open(alloc, path, .{ .resource_manager = &resources });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualSlices(u8, "durable", try read.get(committed_key));
    try std.testing.expectError(error.NotFound, read.get(aborted_key));
    try std.testing.expectEqual(TxnStatus.committed, try manager.getTransactionStatus(committed));
    try std.testing.expectEqual(TxnStatus.aborted, try manager.getTransactionStatus(aborted));
    try std.testing.expect((try manager.loadTransactionRecord(committed)).intents_resolved);
    try std.testing.expect((try manager.loadTransactionRecord(aborted)).intents_resolved);
}

test "retained transaction reservations admit existing prepares release abort and fence untracked roots" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const ns: retained_effects.Namespace = @splat(2);
    try store.put(&internal_keys.identity_namespace_key, &ns);
    const id: TxnId = @splat(5);
    try manager.initTransaction(id, 1);
    try manager.writeIntents(id, &.{.{ .key = "existing", .value = "before admission" }}, &.{});
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try retained_effects.admit(&txn, ns, 1, @splat(1), retained_effects.default_limit);
        try txn.commit();
    }
    try manager.resolveIntents(id, .aborted, 2);
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, 0), (try retained_effects.loadReservations(&txn)).?.bytes);
        try std.testing.expectEqual(@as(u64, 0), (try retained_effects.load(&txn)).?.latest);
    }
    // Simulate a released pre-accounting prepared root. One prefix probe must
    // deny source admission, never scan or silently evict the prepared vote.
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try retained_effects.release(&txn, ns, 1, @splat(1));
        try txn.delete(retained_effects.reservation_key);
        try txn.put(intents_prefix ++ "old", "untracked");
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.RetainedEffectsFull, retained_effects.admit(&txn, ns, 2, @splat(2), retained_effects.default_limit));
    }
    try store.delete(intents_prefix ++ "old");
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try retained_effects.admit(&txn, ns, 2, @splat(2), retained_effects.default_limit);
        try txn.commit();
    }
}

test "retained transaction vector credits replace exactly and survive reopen before abort" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-vector-reserved");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(76);
    const intent: WriteIntent = .{ .key = "row", .value = "{}", .retained_artifact_bytes = 256, .retained_artifact_keys = 2 };
    const expected = 48 + try retainedIntentBytes(alloc, intent);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &@as(retained_effects.Namespace, @splat(1)));
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.initTransaction(id, 1);
        try manager.writeIntents(id, &.{intent}, &.{});
        var smaller = intent;
        smaller.retained_artifact_bytes = 64;
        smaller.retained_artifact_keys = 1;
        try manager.writeIntents(id, &.{ smaller, intent, intent }, &.{});
        const admission = (try manager.loadIntentAdmission(alloc, id)).?;
        try std.testing.expectEqual(expected, admission.reservation());
        try std.testing.expectEqual(@as(u64, 3), admission.retained_keys);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try std.testing.expectEqual(expected, (try manager.loadIntentAdmission(alloc, id)).?.reservation());
        try manager.resolveIntents(id, .aborted, 2);
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 0), (try retained_effects.loadReservations(&read)).?.bytes);
    }
}

test "retained transaction byte bounds count exact physical rows without parsing ordinary values" {
    var tiny: [1]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&tiny);
    const raw: WriteIntent = .{ .key = "binary\x00key", .value = "{\"number\":9007199254740993}" };
    try std.testing.expectEqual(@as(u64, raw.value.?.len + 16 + 2 + internal_keys.encodedComponentLen(raw.key)), try retainedIntentBytes(fixed.allocator(), raw));
    const typed: WriteIntent = .{ .key = "row", .value = "sidecar", .prepared_row = "already encoded defaults and generated values" };
    try std.testing.expectEqual(@as(u64, typed.prepared_row.?.len + 23), try retainedIntentBytes(fixed.allocator(), typed));
    const address = try @import("db/relational_integrity_contract.zig").Address.init(@splat(1), "parent");
    const claim_key = address.claimKey();
    // Physical integrity keys are already encoded; neither JSON parsing nor
    // primary-key escaping belongs in their exact prepared reservation.
    const claim: WriteIntent = .{ .key = &claim_key, .value = "binary\x00\\_edges" };
    try std.testing.expectEqual(@as(u64, 16 + claim_key.len + claim.value.?.len), try retainedIntentBytes(fixed.allocator(), claim));
    try std.testing.expectEqual(@as(u64, 16 + claim_key.len), try retainedIntentBytes(fixed.allocator(), .{ .key = &claim_key, .value = null }));
    const special: WriteIntent = .{ .key = "row", .value = "{\"_edges\":[],\"literal\":\"\\u000b\\u2028\\\\\"}" };
    try std.testing.expect((try retainedIntentBytes(std.testing.allocator, special)) >= special.value.?.len + 23);
    for ([_][]const u8{
        "\x00\x00__metadata__:relational_integrity_activation",
        "\x00\x00__metadata__:relational_integrity_retirement",
        "\x00\x00__metadata__:relational_integrity_generation_retirement",
        "\x00\x00__metadata__:restore_staging_owner",
    }) |key| try std.testing.expectEqual(@as(u64, 0), try retainedIntentBytes(fixed.allocator(), .{ .key = key, .value = "binary\x00\\_edges" }));
}

test "transaction cumulative admission is atomic and membership metadata is incremental" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-admission");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn: TxnId = @splat(19);
    try mgr.initTransaction(txn, 100);
    const first: WriteIntent = .{ .key = "a", .value = "value" };
    const cost = try intentAdmissionBytes(first);
    const limit: MutationExtraBatch = .{ .max_intent_admission_bytes = cost * 2 };
    try mgr.writeIntentsExtraBatch(txn, &.{first}, &.{}, limit);
    try mgr.writeIntentsExtraBatch(txn, &.{.{ .key = "b", .value = "value" }}, &.{}, limit);
    const before = try mgr.loadIntentAdmission(alloc, txn);
    const revision = (try mgr.loadTransactionRecord(txn)).intent_revision;
    try std.testing.expectError(error.TransactionTooLarge, mgr.writeIntentsExtraBatch(txn, &.{.{ .key = "c", .value = "value" }}, &.{}, limit));
    try std.testing.expectEqualDeep(before, try mgr.loadIntentAdmission(alloc, txn));
    try std.testing.expectEqual(revision, (try mgr.loadTransactionRecord(txn)).intent_revision);
    try mgr.checkOrdinaryWriteConflict("c");
    // Retries do not double-charge, including after a configured limit drops.
    try mgr.writeIntentsExtraBatch(txn, &.{first}, &.{}, .{ .max_intent_admission_bytes = 1 });
    try std.testing.expectEqualDeep(before, try mgr.loadIntentAdmission(alloc, txn));
    try mgr.writeIntentsExtraBatch(txn, &.{.{ .key = "a", .value = null }}, &.{}, limit);
    const reduced = (try mgr.loadIntentAdmission(alloc, txn)).?;
    try std.testing.expectEqual(@as(u64, 2), reduced.count);
    try std.testing.expect(reduced.bytes < before.?.bytes);
    // There is no growing whole-key-list value for newly prepared transactions.
    const legacy_key = makeSidecarKey(intent_keys_prefix, txn);
    try std.testing.expectError(error.NotFound, mgr.getAlloc(alloc, &legacy_key));
    const header_key = makeSidecarKey(intent_admission_prefix, txn);
    const header = try mgr.getAlloc(alloc, &header_key);
    defer alloc.free(header);
    try std.testing.expectEqual(@as(usize, 32), header.len);
    try mgr.resolveIntents(txn, .aborted, 200);
    try std.testing.expectEqual(@as(?IntentAdmission, null), try mgr.loadIntentAdmission(alloc, txn));
    const member_key = try TxnManager.makeIntentMemberKey(alloc, txn, "a");
    defer alloc.free(member_key);
    try std.testing.expectError(error.NotFound, mgr.getAlloc(alloc, member_key));
}

test "transaction admission counts retained payload rather than a JSON expansion estimate" {
    const payload = try std.testing.allocator.alloc(u8, 3 * 1024 * 1024);
    defer std.testing.allocator.free(payload);
    const cost = try intentAdmissionBytes(.{ .key = "row", .value = payload, .prepared_row = payload });
    try std.testing.expect(cost >= 2 * payload.len);
    try std.testing.expect(cost < 2 * payload.len + 8192);
}

test "transaction admission rejects before copying payloads and coalesces duplicate keys" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(alloc, &runtime);
    defer manager.deinit();
    const txn: TxnId = @splat(29);
    try manager.initTransaction(txn, 100);
    const payload = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');
    var buffer: [64 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    manager.alloc = fixed.allocator();
    defer manager.alloc = alloc;
    const limit: MutationExtraBatch = .{ .max_intent_admission_bytes = 8192 };
    try std.testing.expectError(error.TransactionTooLarge, manager.writeIntentsExtraBatch(txn, &.{.{ .key = "a", .value = payload }}, &.{}, limit));
    try manager.writeIntentsExtraBatch(txn, &.{ .{ .key = "a", .value = payload }, .{ .key = "a", .value = "small" } }, &.{}, limit);
    var snapshot = try manager.collectIntentBatch(alloc, txn);
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot.writes.len);
    try std.testing.expectEqualStrings("small", snapshot.writes[0].value);
}

test "transaction intent admission releases failed allocations before voting" {
    const Check = struct {
        fn run(failing: Allocator) !void {
            const alloc = std.testing.allocator;
            var backend = mem_backend.Backend.init(alloc, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(alloc, .{});
            defer runtime.deinit();
            var mgr = try TxnManager.init(alloc, &runtime);
            defer mgr.deinit();
            const txn: TxnId = @splat(31);
            try mgr.initTransaction(txn, 100);
            mgr.alloc = failing;
            defer mgr.alloc = alloc;
            try mgr.writeIntentsExtraBatch(txn, &.{.{ .key = "row", .value = "{}", .prepared_row = "physical" }}, &.{}, .{ .max_intent_admission_bytes = 8192 });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "transaction journal refuses to lose unprepared JSON null typing" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();
    var mgr = try TxnManager.init(alloc, &runtime);
    defer mgr.deinit();
    const txn: TxnId = @splat(32);
    try mgr.initTransaction(txn, 100);
    try std.testing.expectError(error.PreparedIntentRequiresMaterialization, mgr.writeIntents(txn, &.{.{ .key = "row", .value = "{\"j\":null}", .json_null_fields = &.{"j"} }}, &.{}));
    // Once the native AROW exists, its bitmap is the durable authority.
    try mgr.writeIntents(txn, &.{.{ .key = "row", .value = "{}", .json_null_fields = &.{"j"}, .prepared_row = "physical" }}, &.{});
}

test "transaction abort retires large intents without loading their payloads" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();
    var mgr = try TxnManager.init(alloc, &runtime);
    defer mgr.deinit();
    const txn: TxnId = @splat(32);
    try mgr.initTransaction(txn, 100);
    const payload = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');
    try mgr.writeIntents(txn, &.{.{ .key = "row", .value = payload }}, &.{});
    var buffer: [64 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    mgr.alloc = fixed.allocator();
    defer mgr.alloc = alloc;
    try mgr.resolveIntents(txn, .aborted, 200);
    try std.testing.expect(!(try mgr.hasIntents(txn)));
}

test "transaction admission converts released document manifests once" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();
    var mgr = try TxnManager.init(alloc, &runtime);
    defer mgr.deinit();
    const txn: TxnId = @splat(33);
    try mgr.initTransaction(txn, 100);
    try mgr.writeIntents(txn, &.{.{ .key = "old", .value = "first" }}, &.{});
    const header_key = makeSidecarKey(intent_admission_prefix, txn);
    const member_key = try TxnManager.makeIntentMemberKey(alloc, txn, "old");
    defer alloc.free(member_key);
    const legacy_key = makeSidecarKey(intent_keys_prefix, txn);
    const legacy_value = try encodeParticipantList(alloc, &.{"old"});
    defer alloc.free(legacy_value);
    try mgr.applyBatch(&.{.{ .key = &legacy_key, .value = legacy_value }}, &.{ &header_key, member_key }, null);
    try mgr.writeIntents(txn, &.{.{ .key = "new", .value = "second" }}, &.{});
    try std.testing.expectEqual(@as(u64, 2), (try mgr.loadIntentAdmission(alloc, txn)).?.count);
    try mgr.writeIntents(txn, &.{.{ .key = "old", .value = "replacement" }}, &.{});
    var snapshot = try mgr.collectIntentBatch(alloc, txn);
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), snapshot.writes.len);
    try mgr.resolveIntents(txn, .committed, 200);
    const value = try getVisibleDocRuntime(&mgr.store, alloc, "old");
    defer alloc.free(value);
    try std.testing.expectEqualStrings("replacement", value);
}

test "transaction prepared envelope preserves borrowed logical and physical views" {
    const bytes = [_]u8{ 2, 2, 0, 0, 0, '{', '}', 'A', 'R', 'O', 'W' };
    const decoded = try IntentValue.decode(&bytes);
    try std.testing.expectEqualStrings("{}", decoded.value.?);
    try std.testing.expectEqualStrings("AROW", decoded.prepared_row.?);
    try std.testing.expect(decoded.value.?.ptr == bytes[5..].ptr);
    try std.testing.expectError(error.InvalidTxnRecord, IntentValue.decode(bytes[0..6]));
    try std.testing.expectError(error.InvalidTxnRecord, IntentValue.decode(&.{ 1, 0 }));
    try std.testing.expectError(error.InvalidTxnRecord, IntentValue.decode(&.{3}));
}

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
        try std.testing.expect(err == error.NotFound);
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

test "transaction intent snapshot owns one payload copy and retirement reads only keys" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-owned-snapshot");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);
    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();
    const txn_id: TxnId = @splat(17);
    try mgr.initTransaction(txn_id, 100);
    const payload = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');
    try mgr.writeIntentsExtraBatch(txn_id, &.{.{ .key = "row", .value = payload }}, &.{}, .{ .schema_binding = .{ .version = 1 } });
    try std.testing.expect(try mgr.hasSchemaLeases());
    try std.testing.expectError(error.SchemaInUse, mgr.writeIntentsExtraBatch(txn_id, &.{}, &.{}, .{ .schema_binding = .{ .version = 2 } }));

    const scratch = try alloc.alloc(u8, payload.len + 32 * 1024);
    defer alloc.free(scratch);
    var fixed = std.heap.FixedBufferAllocator.init(scratch);
    var snapshot = try mgr.collectIntentBatch(fixed.allocator(), txn_id);
    try std.testing.expectEqualSlices(u8, payload, snapshot.writes[0].value);
    try std.testing.expectEqual(snapshot.owned_entries.?[0].value.ptr + 1, snapshot.writes[0].value.ptr);
    try std.testing.expectEqual(@as(?u32, 1), snapshot.schema_binding.?.version);
    const revision = snapshot.revision;
    snapshot.deinit(fixed.allocator());

    const Check = struct {
        fn run(failing: Allocator, manager: *TxnManager, id: TxnId) !void {
            var captured = try manager.collectIntentBatch(failing, id);
            defer captured.deinit(failing);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &mgr, txn_id });

    // Even a 256 KiB intent can retire in a 32 KiB working set: no payload
    // reads/copies are permitted once the caller supplies a fenced snapshot.
    var retirement = std.heap.FixedBufferAllocator.init(scratch[0 .. 32 * 1024]);
    mgr.alloc = retirement.allocator();
    defer mgr.alloc = alloc;
    _ = try mgr.resolveIntentsWithExtraBatch(txn_id, .committed, 200, .{
        .expected_intent_revision = revision,
        .known_intent_keys = &.{"row"},
        .skip_all_intent_application = true,
        .writes = &.{.{ .key = "resolved", .value = "ok" }},
    });
    try std.testing.expect(!try mgr.hasSchemaLeases());
    try std.testing.expect(!try mgr.hasIntents(txn_id));
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

test "transaction resolution supports allocation-free and indexed intent filtering" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-resolution-filtering");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();

    const skip_all_txn: TxnId = .{ 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2 };
    try mgr.initTransaction(skip_all_txn, 1_000);
    try mgr.writeIntents(skip_all_txn, &.{
        .{ .key = "doc:all-a", .value = "legacy-a" },
        .{ .key = "doc:all-b", .value = "legacy-b" },
    }, &.{});
    _ = try mgr.resolveIntentsWithExtraBatch(skip_all_txn, .committed, 2_000, .{
        .writes = &.{.{ .key = "materialized:all", .value = "packed" }},
        .skip_all_intent_application = true,
    });
    try std.testing.expectError(error.NotFound, getVisibleDoc(&store, alloc, "doc:all-a"));
    try std.testing.expectError(error.NotFound, getVisibleDoc(&store, alloc, "doc:all-b"));
    const materialized = try store.get(alloc, "materialized:all");
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("packed", materialized);
    try mgr.checkOrdinaryWriteConflict("doc:all-a");
    try mgr.checkOrdinaryWriteConflict("doc:all-b");

    const selective_txn: TxnId = .{ 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4 };
    try mgr.initTransaction(selective_txn, 3_000);
    try mgr.writeIntents(selective_txn, &.{
        .{ .key = "doc:select-a", .value = "a" },
        .{ .key = "doc:select-b", .value = "b" },
        .{ .key = "doc:select-c", .value = "c" },
    }, &.{});
    _ = try mgr.resolveIntentsWithExtraBatch(selective_txn, .committed, 4_000, .{
        .skip_intent_keys = &.{ "doc:select-a", "doc:select-c" },
    });
    try std.testing.expectError(error.NotFound, getVisibleDoc(&store, alloc, "doc:select-a"));
    const selected = try getVisibleDoc(&store, alloc, "doc:select-b");
    defer alloc.free(selected);
    try std.testing.expectEqualStrings("b", selected);
    try std.testing.expectError(error.NotFound, getVisibleDoc(&store, alloc, "doc:select-c"));
    try mgr.checkOrdinaryWriteConflict("doc:select-a");
    try mgr.checkOrdinaryWriteConflict("doc:select-b");
    try mgr.checkOrdinaryWriteConflict("doc:select-c");
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
    const txn_id: TxnId = @splat(6);
    const participants = [_][]const u8{ "table2:4:docs:group:7", "table2:4:docs:group:8" };
    try mgr.initTransactionWithParticipantsCreatedAtAndRole(txn_id, 1_000, 900, &participants, false);

    var legacy: [txn_record_v3_size]u8 = @splat(0);
    legacy[0] = @backingInt(TxnStatus.pending);
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

    const txn_id_2: TxnId = @splat(7);
    const txn_id_3: TxnId = @splat(8);
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
        try std.testing.expect(err == error.NotFound);
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

    try std.testing.expectError(error.UniqueConstraintViolation, mgr.writeIntents(txn_id, &.{
        .{ .key = "existing_key", .value = "new_value" },
    }, &.{.{ .key = "existing_key", .expected_version = 0, .unique_absence = true }}));
    try std.testing.expectError(error.InvalidArgument, mgr.checkVersionPredicates(
        &.{.{ .key = "existing_key", .expected_version = 5000, .unique_absence = true }},
        null,
    ));

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

test "transaction shared read guards fence writes and survive restart until resolution" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const first: TxnId = @splat(71);
    const second: TxnId = @splat(72);
    const writer: TxnId = @splat(73);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold = 2 });
        defer backend.close();
        const runtime_store = try backend.runtimeStore(alloc, .{});
        var store = try DocStore.openRuntime(alloc, runtime_store);
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try putVisibleDoc(&store, alloc, "parent", "parent row");
        try ttl.writeTimestamp(&store, "parent", 500);
        try manager.initTransaction(first, 600);
        try manager.initTransaction(second, 601);
        try manager.initTransaction(writer, 602);
        const parent = [_]VersionPredicate{.{ .key = "parent", .expected_version = 500 }};
        try manager.writeIntents(first, &.{}, &parent);
        // Shared readers do not serialize all inserts into the same parent.
        try manager.writeIntents(second, &.{}, &parent);
        try std.testing.expect(try manager.hasIntents(first));
        try std.testing.expect(try manager.hasTopologySensitiveTransactions());
        try std.testing.expectEqual(@as(u64, 2), try manager.readGuardCount());
        const before = backend.snapshotMaintenanceStats();
        try std.testing.expectError(error.IntentConflict, manager.checkOrdinaryWriteConflict("parent"));
        try std.testing.expectError(error.IntentConflict, manager.checkOrdinaryWriteConflicts(&.{ "free", "parent" }));
        try std.testing.expectError(error.IntentConflict, manager.writeIntents(writer, &.{.{ .key = "parent", .value = null }}, &.{}));
        try std.testing.expectError(error.IntentConflict, manager.writeIntents(first, &.{.{ .key = "parent", .value = "new" }}, &.{}));
        try manager.checkOrdinaryWriteConflicts(&.{ "parent\x00", "parents", "paren" });
        // This cold-key shared-lock probe must not clone the LSM memtable.
        const after = backend.snapshotMaintenanceStats();
        try std.testing.expectEqual(
            before.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.bound_read_txn)].calls,
            after.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.bound_read_txn)].calls,
        );
        try manager.writeIntents(first, &.{}, &parent);
        try std.testing.expectEqual(@as(u64, 2), try manager.readGuardCount());
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        const runtime_store = try backend.runtimeStore(alloc, .{});
        var store = try DocStore.openRuntime(alloc, runtime_store);
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try std.testing.expectError(error.IntentConflict, manager.checkOrdinaryWriteConflict("parent"));
        try manager.resolveIntents(second, .aborted, 700);
        try std.testing.expectEqual(@as(u64, 1), try manager.readGuardCount());
        // Sole reader can upgrade without releasing its dependency first.
        try manager.writeIntents(first, &.{.{ .key = "parent", .value = "updated" }}, &.{});
        try manager.resolveIntents(first, .committed, 701);
        try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
        try manager.checkOrdinaryWriteConflict("parent");
        try std.testing.expect(!try manager.hasIntents(first));
        const value = try getVisibleDocRuntime(&manager.store, alloc, "parent");
        defer alloc.free(value);
        try std.testing.expectEqualStrings("updated", value);
        try manager.resolveIntents(first, .committed, 701);
        try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
    }
}

test "transaction activated range guards fence pending writers without serializing disjoint writes" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    try store.put(range_protection.activation_key, range_protection.activation_value);
    const first: TxnId = @splat(91);
    const second: TxnId = @splat(92);
    const reader: TxnId = @splat(93);
    try manager.initTransaction(first, 1);
    try manager.initTransaction(second, 1);
    try manager.initTransaction(reader, 1);
    const key = range_protection.counterKey(range_protection.bucket("alpha"));
    const predicate: VersionPredicate = .{ .key = &key, .expected_version = 0, .comparison = .exact_value, .expected_value = null };
    try manager.writeIntents(first, &.{.{ .key = "alpha", .value = "1" }}, &.{});
    // Same bucket, different keys: both writers prepare concurrently.
    try manager.writeIntents(second, &.{.{ .key = "another", .value = "2" }}, &.{});
    try std.testing.expectError(error.IntentConflict, manager.writeIntents(reader, &.{}, &.{predicate}));
    try manager.resolveIntents(first, .aborted, 2);
    try std.testing.expectError(error.IntentConflict, manager.writeIntents(reader, &.{}, &.{predicate}));
    try manager.resolveIntents(second, .aborted, 2);
    try manager.writeIntents(reader, &.{}, &.{predicate});
    try std.testing.expectError(error.IntentConflict, manager.checkOrdinaryWriteConflict("absent-phantom"));
    try manager.checkOrdinaryWriteConflict("different-bucket");
    try manager.resolveIntents(reader, .aborted, 3);
    try manager.checkOrdinaryWriteConflict("absent-phantom");
    try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
}

test "transaction index span guards fence pending and later writers without blocking disjoint tuples" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    try store.put(range_protection.activation_key, range_protection.activation_value);
    const writer: TxnId = @splat(101);
    const reader: TxnId = @splat(102);
    const unrelated: TxnId = @splat(103);
    const later_writer: TxnId = @splat(104);
    try manager.initTransaction(writer, 1);
    try manager.initTransaction(reader, 1);
    try manager.initTransaction(unrelated, 1);
    try manager.initTransaction(later_writer, 1);
    var component: std.ArrayList(u8) = .empty;
    defer component.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&component, alloc, "primary");
    var forward: std.ArrayList(u8) = .empty;
    defer forward.deinit(alloc);
    const forward_prefix = try @import("db/relational_index_records.zig").forwardPrefix(.{ .generation = 7, .slot = 2 });
    try forward.appendSlice(alloc, &forward_prefix);
    try forward.appendSlice(alloc, &.{ 0x80, 'x', 0, 0 });
    try forward.appendSlice(alloc, component.items[1..]);
    var footer: [4]u8 = undefined;
    std.mem.writeInt(u32, &footer, @intCast(component.items.len - 1), .big);
    try forward.appendSlice(alloc, &footer);
    const span = (try range_protection.indexSpanDigest(forward.items)).?;
    const other_span: [range_protection.index_span_digest_bytes]u8 = @splat(8);
    const counter = range_protection.indexCounterKey(span);
    const proof: VersionPredicate = .{ .key = &counter, .expected_version = 0, .comparison = .exact_value, .expected_value = null };
    try manager.writeIntentsExtraBatch(writer, &.{.{ .key = "primary", .value = "{}" }}, &.{}, .{ .index_span_digests = &.{span} });
    try std.testing.expectError(error.IntentConflict, manager.writeIntents(reader, &.{}, &.{proof}));
    try manager.resolveIntents(writer, .aborted, 2);
    try manager.writeIntents(reader, &.{}, &.{proof});
    try std.testing.expectError(error.IntentConflict, manager.checkIndexForwardWriteConflicts(&.{forward.items}));
    try std.testing.expectError(error.IntentConflict, manager.writeIntentsExtraBatch(later_writer, &.{.{ .key = "another", .value = "{}" }}, &.{}, .{ .index_span_digests = &.{span} }));
    try manager.writeIntentsExtraBatch(unrelated, &.{.{ .key = "another", .value = "{}" }}, &.{}, .{ .index_span_digests = &.{other_span} });
    try manager.resolveIntents(reader, .aborted, 3);
    try manager.checkIndexForwardWriteConflicts(&.{forward.items});
    try manager.resolveIntents(unrelated, .aborted, 3);
    try manager.resolveIntents(later_writer, .aborted, 3);
    try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
}

test "transaction activated range reader and writer reservations recover across LSM restart" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const writer: TxnId = @splat(94);
    const reader: TxnId = @splat(95);
    const key = range_protection.counterKey(range_protection.bucket("alpha"));
    const predicate: VersionPredicate = .{ .key = &key, .expected_version = 0, .comparison = .exact_value, .expected_value = null };
    for (0..3) |phase| {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        switch (phase) {
            0 => {
                try store.put(range_protection.activation_key, range_protection.activation_value);
                try manager.initTransaction(writer, 1);
                try manager.initTransaction(reader, 1);
                try manager.writeIntents(writer, &.{.{ .key = "alpha", .value = "pending" }}, &.{});
            },
            1 => {
                try std.testing.expectError(error.IntentConflict, manager.writeIntents(reader, &.{}, &.{predicate}));
                try manager.resolveIntents(writer, .aborted, 2);
                try manager.writeIntents(reader, &.{}, &.{predicate});
            },
            2 => {
                try std.testing.expectError(error.IntentConflict, manager.checkOrdinaryWriteConflict("another phantom"));
                try std.testing.expect(try manager.hasTopologySensitiveTransactions());
                try manager.resolveIntents(reader, .aborted, 3);
                try manager.checkOrdinaryWriteConflict("another phantom");
                try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
            },
            else => unreachable,
        }
    }
}

test "transaction range generations are atomic snapshot bound and coalesced per bucket" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const a = try internal_keys.documentKeyAlloc(alloc, "alpha");
    defer alloc.free(a);
    const b = try internal_keys.documentKeyAlloc(alloc, "another");
    defer alloc.free(b);
    const id = range_protection.bucket("alpha");
    try store.put(a, "1");
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(null, try range_protection.generation(&read, id));
        try std.testing.expectError(error.SqlRangeTrackingRequired, range_protection.capture(alloc, &read, "", ""));
    }
    try store.put(range_protection.activation_key, range_protection.activation_value);
    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(a, "2");
        try batch.put(b, "3");
        try batch.commit();
    }
    var pinned = try store.beginReadTxn();
    defer pinned.abort();
    try std.testing.expectEqual(@as(?u64, 1), try range_protection.generation(&pinned, id));
    {
        var batch = try store.beginWriteBatch();
        defer batch.abort();
        try batch.put(a, "aborted");
    }
    try store.delete(a);
    var current = try store.beginReadTxn();
    defer current.abort();
    try std.testing.expectEqual(@as(?u64, 1), try range_protection.generation(&pinned, id));
    try std.testing.expectEqual(@as(?u64, 2), try range_protection.generation(&current, id));
    const proofs = try range_protection.capture(alloc, &current, "alpha", "azure");
    defer alloc.free(proofs);
    try std.testing.expectEqual(@as(usize, 1), proofs.len);
    try std.testing.expectEqual(@as(?u64, 2), proofs[0].generation);
}

test "transaction read guards protect absence and reject write skew" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(alloc, &runtime);
    defer manager.deinit();
    const a: TxnId = @splat(81);
    const b: TxnId = @splat(82);
    try manager.initTransaction(a, 100);
    try manager.initTransaction(b, 100);
    try manager.writeIntents(a, &.{.{ .key = "a", .value = "a" }}, &.{.{ .key = "b", .expected_version = 0 }});
    try std.testing.expectError(error.IntentConflict, manager.writeIntents(b, &.{.{ .key = "b", .value = "b" }}, &.{}));
    try manager.resolveIntents(a, .committed, 200);
    try manager.writeIntents(b, &.{.{ .key = "b", .value = "b" }}, &.{});
    try manager.resolveIntents(b, .aborted, 201);
    try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
}

test "transaction read guard admission is cumulative retry safe and atomic on allocation failure" {
    const Check = struct {
        fn run(failing: Allocator) !void {
            const alloc = std.testing.allocator;
            var backend = mem_backend.Backend.init(alloc, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(alloc, .{});
            defer runtime.deinit();
            var manager = try TxnManager.init(alloc, &runtime);
            defer manager.deinit();
            const id: TxnId = @splat(83);
            try manager.initTransaction(id, 100);
            manager.alloc = failing;
            defer manager.alloc = alloc;
            manager.writeIntentsExtraBatch(id, &.{}, &.{ .{ .key = "a", .expected_version = 0 }, .{ .key = "a", .expected_version = 0 } }, .{ .max_intent_admission_bytes = 8192 }) catch |err| {
                manager.alloc = alloc;
                try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
                try std.testing.expect(!(try manager.loadTransactionRecord(id)).prepared);
                try manager.checkOrdinaryWriteConflict("a");
                return err;
            };
            manager.alloc = alloc;
            try std.testing.expectEqual(@as(u64, 1), try manager.readGuardCount());
            try manager.writeIntentsExtraBatch(id, &.{}, &.{.{ .key = "a", .expected_version = 0 }}, .{ .max_intent_admission_bytes = 1 });
            try std.testing.expectError(error.TransactionTooLarge, manager.writeIntentsExtraBatch(id, &.{}, &.{.{ .key = "b", .expected_version = 0 }}, .{ .max_intent_admission_bytes = 8192 }));
            try std.testing.expectEqual(@as(u64, 1), try manager.readGuardCount());
            try manager.checkOrdinaryWriteConflict("b");
            try manager.resolveIntents(id, .aborted, 200);
            try std.testing.expectEqual(@as(u64, 0), try manager.readGuardCount());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
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

test "transaction point reads do not clone runtime lsm mutable state" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var backend = try lsm_backend.Backend.open(alloc, path, .{
        .flush_threshold_bytes = 64 * 1024 * 1024,
    });
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try DocStore.openRuntime(alloc, runtime_store);
    defer store.close();
    var mgr = try TxnManager.init(alloc, &store);
    defer mgr.deinit();

    const txn_id: TxnId = @splat(3);
    try mgr.initTransaction(txn_id, 1000);
    try mgr.writeIntents(txn_id, &.{.{ .key = "shared", .value = "pending" }}, &.{});
    const before = backend.snapshotMaintenanceStats();
    try std.testing.expectError(TxnError.IntentConflict, mgr.checkOrdinaryWriteConflict("shared"));
    try mgr.checkOrdinaryWriteConflict("unlocked");
    const before_batch_reads = backend.snapshotReadStats();
    try std.testing.expectError(
        TxnError.IntentConflict,
        mgr.checkOrdinaryWriteConflicts(&.{ "free:a", "shared", "free:b" }),
    );
    try mgr.checkOrdinaryWriteConflicts(&.{ "free:a", "free:b" });
    const after_batch_reads = backend.snapshotReadStats();
    try std.testing.expectEqual(
        before_batch_reads.get_many_sorted_calls + 2,
        after_batch_reads.get_many_sorted_calls,
    );
    try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(txn_id));
    const after_point_reads = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(
        before.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.bound_read_txn)].calls,
        after_point_reads.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.bound_read_txn)].calls,
    );

    try std.testing.expect(try mgr.hasIntents(txn_id));
    var intent_batch = try mgr.collectIntentBatch(alloc, txn_id);
    defer intent_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), intent_batch.writes.len);
    try mgr.resolveIntents(txn_id, .aborted, 2000);
    try std.testing.expect(!try mgr.hasIntents(txn_id));
    const after_lifecycle = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(
        before.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.bound_read_txn)].calls,
        after_lifecycle.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.bound_read_txn)].calls,
    );
}

test "transaction intent manifest rolls forward legacy in-flight intents" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var mgr = try TxnManager.init(alloc, &runtime);
    defer mgr.deinit();

    const txn_id: TxnId = @splat(5);
    try mgr.initTransaction(txn_id, 1000);
    try mgr.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "a" }}, &.{});

    // Simulate an in-flight transaction prepared by a pre-manifest binary.
    const manifest_key = makeSidecarKey(intent_keys_prefix, txn_id);
    try mgr.applyBatch(&.{}, &.{&manifest_key}, null);
    try mgr.writeIntents(txn_id, &.{.{ .key = "doc:b", .value = "b" }}, &.{});

    var snapshot = try mgr.collectIntentBatch(alloc, txn_id);
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), snapshot.writes.len);
    try std.testing.expectEqualStrings("doc:a", snapshot.writes[0].key);
    try std.testing.expectEqualStrings("doc:b", snapshot.writes[1].key);
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
        try std.testing.expect(err == error.NotFound);
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
        try std.testing.expect(err == error.NotFound);
    };

    const intent_key = try mgr.makeIntentKey(txn_id, "doc:stale_pending");
    defer alloc.free(intent_key);
    _ = store.get(alloc, intent_key) catch |err| {
        try std.testing.expect(err == error.NotFound);
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
    const txn_id: TxnId = @splat(9);
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
        try std.testing.expect(err == error.NotFound);
    };
    try std.testing.expectError(TxnError.TxnNotFound, mgr.getTransactionStatus(txn_id));
}

test "transaction participant resolution releases allocations once on every failure" {
    const Check = struct {
        fn run(failing: Allocator) !void {
            const alloc = std.testing.allocator;
            var backend = mem_backend.Backend.init(alloc, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(alloc, .{});
            defer runtime.deinit();
            var mgr = try TxnManager.init(alloc, &runtime);
            defer mgr.deinit();
            const txn: TxnId = @splat(38);
            try mgr.initTransactionWithParticipants(txn, 100, &.{ "first", "second" });
            try mgr.markParticipantResolved(txn, "first");
            mgr.alloc = failing;
            defer mgr.alloc = alloc;
            try mgr.markParticipantResolved(txn, "second");
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
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
    const outbox_key = makeTransactionReplicationBatchOutboxKey(txn_id);
    try mgr.putValue(&outbox_key, "pending-mirror-delivery");
    try std.testing.expect(try mgr.hasTopologySensitiveTransactions());
    try mgr.clearReplicationOutbox(txn_id, .batch);
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
        error.NotFound => null,
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
    const txn_id: TxnId = @splat(5);
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

test "transaction participant batch migrates legacy evidence atomically and cleans indexed membership" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();
    var mgr = try TxnManager.init(alloc, &runtime);
    defer mgr.deinit();
    const txn: TxnId = @splat(41);
    try mgr.initTransactionWithParticipants(txn, 100, &.{ "a", "b", "c" });
    try mgr.markParticipantResolved(txn, "a");
    try std.testing.expectError(error.InvalidParticipant, mgr.markParticipantsResolvedExtraBatch(txn, &.{ "b", "absent" }, .{}));
    const index_key = makeSidecarKey(participant_index_prefix, txn);
    try std.testing.expect(!(try mgr.keyExists(&index_key)));
    try mgr.markParticipantsResolvedExtraBatch(txn, &.{ "b", "b" }, .{});
    const first = try mgr.getResolvedParticipants(alloc, txn);
    defer freeParticipantList(alloc, first);
    try std.testing.expectEqual(@as(usize, 2), first.len);
    try std.testing.expectEqualStrings("a", first[0]);
    try std.testing.expectEqualStrings("b", first[1]);
    try mgr.markParticipantsResolvedExtraBatch(txn, &.{ "b", "c" }, .{});
    try mgr.markParticipantResolved(txn, "c");
    const unresolved = try mgr.getUnresolvedParticipants(alloc, txn);
    defer freeParticipantList(alloc, unresolved);
    try std.testing.expectEqual(@as(usize, 0), unresolved.len);
    const bytes = try mgr.getAlloc(alloc, &index_key);
    defer alloc.free(bytes);
    const index = try ParticipantIndex.decode(bytes);
    try std.testing.expectEqual(@as(u64, 3), index.enlisted);
    try std.testing.expectEqual(@as(u64, 3), index.resolved);
    try mgr.deleteTransactionMetadata(txn);
    try std.testing.expect(!(try mgr.keyExists(&index_key)));
    const resolutions = try mgr.scanPrefix(alloc, &makeSidecarKey(resolved_index_prefix, txn));
    defer backend_scan.freeResults(alloc, resolutions);
    try std.testing.expectEqual(@as(usize, 0), resolutions.len);
    try std.testing.expectError(error.TxnNotFound, mgr.markParticipantsResolvedExtraBatch(txn, &.{"a"}, .{}));
}

test "transaction participant batch releases migration allocations on every failure" {
    const Check = struct {
        fn run(failing: Allocator) !void {
            const alloc = std.testing.allocator;
            var backend = mem_backend.Backend.init(alloc, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(alloc, .{});
            defer runtime.deinit();
            var mgr = try TxnManager.init(alloc, &runtime);
            defer mgr.deinit();
            const txn: TxnId = @splat(42);
            try mgr.initTransactionWithParticipants(txn, 100, &.{ "a", "b", "c" });
            try mgr.markParticipantResolved(txn, "a");
            mgr.alloc = failing;
            defer mgr.alloc = alloc;
            try mgr.markParticipantsResolvedExtraBatch(txn, &.{ "b", "c" }, .{});
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "transaction intent collection original admission rejects every phase and retries exact owned data" {
    try testIntentCollectionOriginalAdmission(false);
    try testIntentCollectionOriginalAdmission(true);
}

fn testIntentCollectionOriginalAdmission(persisted: bool) !void {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-collection-controls");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = if (persisted) 1024 else 64 * 1024 * 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const id: TxnId = @splat(0x87);
    try manager.initTransaction(id, 100);
    const payload = try alloc.alloc(u8, 192 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'q');
    var writes: [71]WriteIntent = undefined;
    var keys: [70][8]u8 = undefined;
    for (writes[0..70], &keys, 0..) |*write, *buffer, i| {
        const key = try std.fmt.bufPrint(buffer, "k{d:0>4}", .{i});
        write.* = .{ .key = key, .value = if (i == 0) payload else "small" };
    }
    writes[70] = .{ .key = "deleted", .value = null };
    try manager.writeIntents(id, &writes, &.{});
    if (persisted) try backend.flushBufferedWritesWithOptions(.{ .compact = false });
    try std.testing.expectEqual(persisted, backend.runs.count() > 0);
    const State = struct {
        checks: usize = 0,
        stop_at: usize,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks >= self.stop_at) return error.DeadlineExceeded;
        }
    };
    var completed: State = .{ .stop_at = std.math.maxInt(usize) };
    var source: read_control.Payload = .{ .ptr = &completed, .vtable = &.{ .checkpoint = State.check } };
    var snapshot = try manager.collectIntentBatchWithAdmission(alloc, id, read_control.Port.init(&source));

    try std.testing.expectEqual(@as(usize, 70), snapshot.writes.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.deletes.len);
    try std.testing.expectEqualSlices(u8, payload, snapshot.writes[0].value);
    try std.testing.expectEqual(snapshot.owned_entries.?[1].value.ptr + 1, snapshot.writes[0].value.ptr);
    const revision = snapshot.revision;
    snapshot.deinit(alloc);
    // Current-scan rotation and cold index/block admission happen only on the
    // first pass. Warm the physical view before enumerating a stable path.
    completed.checks = 0;
    var warmed = try manager.collectIntentBatchWithAdmission(alloc, id, read_control.Port.init(&source));
    const checks = completed.checks;
    try std.testing.expect(checks > 70);
    warmed.deinit(alloc);
    // Fail at every reachable lifetime check, including after probe batches
    // and partial large copies. TestingAllocator proves exact rollback ownership.
    for (1..checks + 1) |stop_at| {
        var stopped: State = .{ .stop_at = stop_at };
        source.ptr = &stopped;
        try std.testing.expectError(error.DeadlineExceeded, manager.collectIntentBatchWithAdmission(alloc, id, read_control.Port.init(&source)));
        try std.testing.expectEqual(stop_at, stopped.checks);
    }
    completed.checks = 0;
    source.ptr = &completed;
    var retry = try manager.collectIntentBatchWithAdmission(alloc, id, read_control.Port.init(&source));
    defer retry.deinit(alloc);
    try std.testing.expectEqual(revision, retry.revision);
    try std.testing.expectEqualSlices(u8, payload, retry.writes[0].value);
    try std.testing.expectEqualSlices(u8, "deleted", retry.deletes[0]);
}

test "transaction intent collection original admission retains legacy sidecars and prefix scan ownership" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-legacy-collection-controls");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const id: TxnId = @splat(0x88);
    try manager.initTransaction(id, 100);
    try manager.writeIntents(id, &.{ .{ .key = "a", .value = "value-a" }, .{ .key = "b", .value = "value-b" }, .{ .key = "deleted", .value = null } }, &.{});
    const admission_key = makeSidecarKey(intent_admission_prefix, id);
    const legacy_key = makeSidecarKey(intent_keys_prefix, id);
    const legacy = try encodeParticipantList(alloc, &.{ "a", "b", "deleted" });
    defer alloc.free(legacy);
    try manager.applyBatch(&.{.{ .key = &legacy_key, .value = legacy }}, &.{&admission_key}, null);
    const State = struct {
        checks: usize = 0,
        stop_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks >= self.stop_at) return error.DeadlineExceeded;
        }
    };
    // Both pre-admission-format key lists and databases without a sidecar
    // preserve the same intent revision and payload ownership.
    for (0..2) |mode| {
        if (mode == 1) try manager.applyBatch(&.{}, &.{&legacy_key}, null);
        var state: State = .{};
        var source: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
        var snapshot = try manager.collectIntentBatchWithAdmission(alloc, id, read_control.Port.init(&source));

        try std.testing.expectEqual(@as(usize, 2), snapshot.writes.len);
        try std.testing.expectEqual(@as(usize, 1), snapshot.deletes.len);
        try std.testing.expectEqualSlices(u8, "value-a", snapshot.writes[0].value);
        snapshot.deinit(alloc);
        state.checks = 0;
        var warmed = try manager.collectIntentBatchWithAdmission(alloc, id, read_control.Port.init(&source));
        const checks = state.checks;
        warmed.deinit(alloc);
        for (1..checks + 1) |stop_at| {
            var stopped: State = .{ .stop_at = stop_at };
            source.ptr = &stopped;
            try std.testing.expectError(error.DeadlineExceeded, manager.collectIntentBatchWithAdmission(alloc, id, read_control.Port.init(&source)));
        }
        const Allocation = struct {
            fn run(failing: Allocator, owner: *TxnManager, txn: TxnId, control: read_control.Port) !void {
                var owned = try owner.collectIntentBatchWithAdmission(failing, txn, control);
                defer owned.deinit(failing);
            }
        };
        state.checks = 0;
        source.ptr = &state;
        try std.testing.checkAllAllocationFailures(alloc, Allocation.run, .{ &manager, id, read_control.Port.init(&source) });
    }
}

test "transaction intent collection original admission scopes bound payload and backend contention" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-scope-admission");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    var backend = try lsm_backend.Backend.open(alloc, path, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const Target = enum { payload, backend };
    const Kind = enum { read, probe, current };
    const State = struct {
        store: *DocStore,
        backend: *lsm_backend.Backend,
        target: Target,
        expire: bool,
        expired: bool = false,
        waits: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.expired) return error.DeadlineExceeded;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            if (self.expire) {
                self.expired = true;
            } else self.release();
        }
        fn release(self: *@This()) void {
            switch (self.target) {
                .payload => self.store.payload_policy_mutex.unlock(std.testing.io),
                .backend => self.backend.mu.unlock(),
            }
        }
        fn open(owner: *backend_erased.Store, kind: Kind, control: read_control.Port) !void {
            switch (kind) {
                .read => {
                    var scope = try owner.beginReadWithAdmission(.retain, control);
                    defer scope.abort();
                },
                .probe => {
                    var scope = try owner.beginProbeWithAdmission(.retain, control);
                    defer scope.abort();
                },
                .current => {
                    var scope = try owner.beginCurrentScanWithAdmission(control);
                    defer scope.abort();
                },
            }
        }
    };
    for (std.enums.values(Target)) |target| for (std.enums.values(Kind)) |kind| for ([_]bool{ false, true }) |expire| {
        var state: State = .{ .store = &store, .backend = &backend, .target = target, .expire = expire };
        const locked = switch (target) {
            .payload => store.payload_policy_mutex.tryLock(),
            .backend => backend.mu.tryLock(),
        };
        try std.testing.expect(locked);
        const source: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .wait_for_progress = State.wait } };
        if (expire) {
            try std.testing.expectError(error.DeadlineExceeded, State.open(&manager.store, kind, read_control.Port.init(&source)));
            // A rejected waiter cannot release the fixture's lock ownership.
            const stolen = switch (target) {
                .payload => store.payload_policy_mutex.tryLock(),
                .backend => backend.mu.tryLock(),
            };
            try std.testing.expect(!stolen);
            state.release();
        } else try State.open(&manager.store, kind, read_control.Port.init(&source));
        try std.testing.expectEqual(@as(usize, 1), state.waits);
        try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    };

    // An already-open live probe must use the same bounded backend capture
    // lock. Its output references are discarded on expiry, with no fresh scope.
    try store.put("probe:scope", "value");
    for ([_]bool{ false, true }) |expire| {
        {
            var state: State = .{ .store = &store, .backend = &backend, .target = .backend, .expire = expire };
            const source: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .wait_for_progress = State.wait } };
            const control = read_control.Port.init(&source);
            var probe = try manager.store.beginProbeWithAdmission(.retain, control);
            defer probe.abort();
            try std.testing.expect(backend.mu.tryLock());
            var values: [1]?[]const u8 = .{null};
            if (expire) {
                try std.testing.expectError(error.DeadlineExceeded, probe.getManySortedWithAdmission(&.{"probe:scope"}, &values, control));
                try std.testing.expect(values[0] == null);
                try std.testing.expect(!backend.mu.tryLock());
                state.release();
            } else {
                try probe.getManySortedWithAdmission(&.{"probe:scope"}, &values, control);
                try std.testing.expectEqualStrings("value", values[0].?);
            }
            try std.testing.expectEqual(@as(usize, 1), state.waits);
        }
        try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    }
    // Reject unsupported CPU transfer before taking any payload/backend lease.
    const Cpu = struct {
        fn transfer(_: *anyopaque) !void {
            @panic("CPU ownership transferred under an incomplete service graph");
        }
    };
    var state: State = .{ .store = &store, .backend = &backend, .target = .backend, .expire = false };
    const source: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .wait_for_progress = State.wait }, .execution_wait = .{ .ptr = &state, .begin = Cpu.transfer, .end = Cpu.transfer } };
    for (std.enums.values(Kind)) |kind| {
        try std.testing.expectError(error.ExecutionWaitUnsupported, State.open(&manager.store, kind, read_control.Port.init(&source)));
        try std.testing.expectEqual(@as(usize, 0), state.waits);
        try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    }
}

test "transaction intent collection original admission lazy layout builder wait preserves pinned epoch" {
    const alloc = std.testing.allocator;
    const runtime = @import("lsm_backend/runtime.zig");
    const storage = @import("lsm_backend/storage_io.zig");
    for ([_]bool{ false, true }) |with_io| for ([_]bool{ false, true }) |expire| {
        const path = try tempTestPath(alloc, "txn-layout-builder");
        defer alloc.free(path);
        defer cleanupTestDir(path);
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .read_runtime = if (with_io) storage.ReadRuntime.init(std.testing.io) else null });
        defer backend.close();
        {
            var write = try backend.beginWrite();
            errdefer write.abort();
            try write.put(.{}, "before", "old");
            try write.commit();
        }
        try backend.flushBufferedWritesWithOptions(.{ .compact = false });
        try std.testing.expect(backend.mu.tryLock());
        defer backend.unlockWithReclamation();
        const version = try backend.createReadVersionFromDirectory();
        _ = version.references.fetchAdd(1, .monotonic);
        defer version.release(&backend);
        const captured_count = version.directory.?.count();
        try std.testing.expect(!version.preparing);
        version.preparing = true;
        const State = struct {
            backend: *lsm_backend.Backend,
            version: *runtime.ReadVersion,
            expire: bool,
            expired: bool = false,
            waits: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.expired) return error.DeadlineExceeded;
            }
            fn unlock(self: *@This()) void {
                self.version.preparing = false;
            }
            fn transfer(_: *anyopaque) !void {}
            fn wait(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.waits += 1;
                // The actual builder lock wait has released the backend lock.
                try std.testing.expect(self.backend.mu.tryLock());
                self.backend.mu.unlock();
                if (self.expire) {
                    self.expired = true;
                } else {
                    var write = try self.backend.beginWrite();
                    errdefer write.abort();
                    try write.put(.{}, "after", "new");
                    try write.commit();
                    try self.backend.flushBufferedWritesWithOptions(.{ .compact = false });
                    try std.testing.expect(self.backend.mu.tryLock());
                    self.unlock();
                    self.backend.mu.unlock();
                }
            }
        };
        var state: State = .{ .backend = &backend, .version = version, .expire = expire };
        const source: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check, .wait_for_progress = State.wait }, .execution_wait = .{ .ptr = &state, .begin = State.transfer, .end = State.transfer } };
        if (expire) {
            try std.testing.expectError(error.DeadlineExceeded, backend.prepareReadVersionWithAdmission(version, read_control.Port.init(&source)));
            try std.testing.expect(!version.prepared);
            state.unlock();
        } else {
            try backend.prepareReadVersionWithAdmission(version, read_control.Port.init(&source));
            try std.testing.expect(version.prepared);
            try std.testing.expectEqual(captured_count, version.runs.len);
            try std.testing.expect(backend.runs.count() > captured_count);
        }
        try std.testing.expectEqual(@as(usize, 1), state.waits);
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
        // Even rejected preparation returns the caller's backend lock.
        try std.testing.expect(!backend.mu.tryLock());
    };
}

test "transaction intent collection original admission lazy projection rejects every checkpoint and allocation" {
    const alloc = std.testing.allocator;
    const runtime = @import("lsm_backend/runtime.zig");
    const path = try tempTestPath(alloc, "txn-layout-projection");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    var backend = try lsm_backend.Backend.open(alloc, path, .{ .compact_threshold_runs = 1024, .l0_soft_limit_runs = 1024, .l0_hard_limit_runs = 2048 });
    defer backend.close();
    for (0..8) |i| {
        var key: [16]u8 = undefined;
        var write = try backend.beginWrite();
        errdefer write.abort();
        try write.put(.{}, try std.fmt.bufPrint(&key, "k{d:0>4}", .{i}), "value");
        try write.commit();
        try backend.flushBufferedWritesWithOptions(.{ .compact = false });
    }
    try std.testing.expect(backend.runs.count() >= 8);
    try std.testing.expect(backend.mu.tryLock());
    const version = try backend.createReadVersionFromDirectory();
    _ = version.references.fetchAdd(1, .monotonic);
    backend.mu.unlock();
    defer {
        const locked = runtime.lockBackend(lsm_backend.Backend, &backend);
        version.release(&backend);
        runtime.unlockBackend(lsm_backend.Backend, &backend, locked);
    }
    const directory = version.directory.?;
    const State = struct {
        checks: usize = 0,
        stop_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks >= self.stop_at) return error.DeadlineExceeded;
        }
    };
    var state: State = .{};
    var source: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
    const projection = try runtime.ReadVersion.createFromDirectoryWithAdmission(alloc, directory, read_control.Port.init(&source));
    const checks = state.checks;
    try std.testing.expect(checks > directory.count());
    try std.testing.expectEqual(directory.count(), projection.runs.len);
    projection.discardProjection();
    for (1..checks + 1) |stop_at| {
        var stopped: State = .{ .stop_at = stop_at };
        source.ptr = &stopped;
        try std.testing.expectError(error.DeadlineExceeded, runtime.ReadVersion.createFromDirectoryWithAdmission(alloc, directory, read_control.Port.init(&source)));
        try std.testing.expectEqual(stop_at, stopped.checks);
        try std.testing.expectEqual(@as(usize, 8), directory.count());
    }
    const Allocation = struct {
        fn run(failing: Allocator, root: *@import("lsm_backend/run_directory.zig").Directory, control: read_control.Port) !void {
            const result = try runtime.ReadVersion.createFromDirectoryWithAdmission(failing, root, control);
            defer result.discardProjection();
        }
    };
    state.checks = 0;
    source.ptr = &state;
    try std.testing.checkAllAllocationFailures(alloc, Allocation.run, .{ directory, read_control.Port.init(&source) });
}

test "prepared transaction effects original rejection cannot become absent state" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(95);
    const catalogs: [32]u8 = @splat(21);
    try manager.initTransaction(id, 1);
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
    const contract = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    const Control = struct {
        checks: usize = 0,
        reject: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject) return error.NotFound;
        }
    };
    var c: Control = .{ .reject = 1 };
    const source: read_control.Payload = .{ .ptr = &c, .vtable = &.{ .checkpoint = Control.check } };
    const port = read_control.Port.init(&source);
    try std.testing.expectError(error.NotFound, manager.loadPreparedEffects(a, id, port));
    c = .{};
    try manager.sealPreparedEffects(id, frame, catalogs, .deferred, port);
    const seal_checks = c.checks;
    const key = makePreparedEffectsKey(id);
    try manager.applyBatch(&.{}, &.{&key}, null);
    for (1..seal_checks + 1) |reject| {
        c = .{ .reject = reject };
        try std.testing.expectError(error.NotFound, manager.sealPreparedEffects(id, frame, catalogs, .deferred, port));
        try std.testing.expect(!try manager.hasPreparedEffects(id));
    }
    try manager.sealPreparedEffects(id, frame, catalogs, .deferred, null);
    c = .{};
    var loaded = (try manager.loadPreparedEffects(a, id, port)).?;
    const load_checks = c.checks;
    loaded.deinit();
    for (1..load_checks + 1) |reject| {
        c = .{ .reject = reject };
        try std.testing.expectError(error.NotFound, manager.loadPreparedEffects(a, id, port));
        try std.testing.expect(try manager.hasPreparedEffects(id));
    }
    try manager.resolveIntents(id, .aborted, 100);
}

test "prepared transaction effects terminal decisions retain priority and orphan plans stay fenced" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const catalogs: [32]u8 = @splat(22);
    for ([_]TxnStatus{ .aborted, .committed }, 0..) |status, i| {
        const id: TxnId = @splat(@as(u8, @intCast(96 + i)));
        try manager.initTransaction(id, 1);
        try manager.writeIntents(id, &.{.{ .key = if (i == 0) "aborted" else "committed", .value = "{}" }}, &.{});
        const contract = try manager.preparedEffectsContract(id, catalogs, .deferred, null);
        const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
        defer a.free(frame);
        try manager.sealPreparedEffects(id, frame, catalogs, .deferred, null);
        if (status == .aborted) {
            var record = try manager.loadTransactionRecord(id);
            record.status = .aborted;
            record.finalized_at = 10;
            try manager.saveTransactionRecord(makeRecordKey(id), record);
            try std.testing.expectError(TxnError.DecisionConflict, manager.resolveIntents(id, .committed, 10));
            try manager.resolveIntents(id, .aborted, 10);
        } else {
            var plan = (try manager.loadPreparedEffects(a, id, null)).?;
            defer plan.deinit();
            _ = try manager.resolvePreparedIntents(id, 10, &plan, .{}, null);
        }
        // Simulate an old writer leaving an opaque sidecar after terminal
        // resolution. An abort can retire it; a committed record cannot prove
        // the plan was consumed and must remain fenced for repair.
        const key = makePreparedEffectsKey(id);
        try manager.applyBatch(&.{.{ .key = &key, .value = frame }}, &.{}, null);
        if (status == .aborted) {
            try manager.resolveIntents(id, .aborted, 10);
            try std.testing.expect(!try manager.hasPreparedEffects(id));
        } else {
            try std.testing.expectError(TxnError.DecisionConflict, manager.resolveIntents(id, .aborted, 10));
            try std.testing.expectError(error.InvalidTxnRecord, manager.resolveIntents(id, .committed, 10));
            try std.testing.expect(!try manager.cleanupTransactionMetadataIfEligibleExtraBatch(id, 100, 100, .{}));
            try std.testing.expect(try manager.hasPreparedEffects(id));
        }
        try std.testing.expect(!@import("portable_backup.zig").isPortableMetadataKey(&key));
    }
}

test "prepared transaction effects recovery retains committed obligations and drains legacy" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const required: TxnId = @splat(0xd1);
    const framed: TxnId = @splat(0xd2);
    const aborted: TxnId = @splat(0xd3);
    const legacy: TxnId = @splat(0xd4);
    try manager.initTransactionRequiringPreparedEffects(required, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
    try manager.writeIntents(required, &.{}, &.{});
    var record = try manager.loadTransactionRecord(required);
    try applyResolveDecision(&record, .committed, 10);
    try manager.saveTransactionRecord(makeRecordKey(required), record);
    try manager.initTransaction(framed, 1);
    try manager.writeIntents(framed, &.{}, &.{});
    const contract = try manager.preparedEffectsContract(framed, @splat(72), .deferred, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try manager.sealPreparedEffects(framed, frame, @splat(72), .deferred, null);
    record = try manager.loadTransactionRecord(framed);
    try applyResolveDecision(&record, .committed, 10);
    record.intents_resolved = true;
    record.intents_resolved_known = true;
    try manager.saveTransactionRecord(makeRecordKey(framed), record);
    try manager.initTransactionRequiringPreparedEffects(aborted, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
    record = try manager.loadTransactionRecord(aborted);
    try applyResolveDecision(&record, .aborted, 10);
    record.intents_resolved = true;
    record.intents_resolved_known = true;
    try manager.saveTransactionRecord(makeRecordKey(aborted), record);
    var corrupt = (prepared_effects.Requirement{ .mode = .synchronous }).encode();
    corrupt[39] ^= 1;
    const aborted_requirement = makePreparationRequirementKey(aborted);
    try manager.applyBatch(&.{.{ .key = &aborted_requirement, .value = &corrupt }}, &.{}, null);
    try manager.initTransaction(legacy, 1);
    try manager.writeIntents(legacy, &.{.{ .key = "legacy-visible", .value = "committed" }}, &.{});
    try manager.resolveIntents(legacy, .committed, 10);
    const stats = try manager.recoverTransactions(100, 101);
    try std.testing.expectEqual(@as(u64, 2), stats.deferred_unresolved);
    try std.testing.expectEqual(@as(u64, 1), stats.resolved_finalized);
    try std.testing.expectEqual(@as(u64, 1), stats.cleaned_records);
    try std.testing.expect(try manager.hasPreparationRequirement(required));
    try std.testing.expect(!(try manager.loadTransactionRecord(required)).intents_resolved);
    try std.testing.expect(try manager.hasPreparedEffects(framed));
    try std.testing.expect(!try manager.hasPreparationRequirement(aborted));
    const visible = try getVisibleDocRuntime(&manager.store, a, "legacy-visible");
    defer a.free(visible);
    try std.testing.expectEqualStrings("committed", visible);
    const next = try manager.recoverTransactions(100, 102);
    try std.testing.expectEqual(@as(u64, 2), next.deferred_unresolved);
    try std.testing.expectEqual(@as(u64, 1), next.cleaned_records);
}

test "prepared transaction effects terminal empty intents still consume accepted materialization once" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(0xd5);
    try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
    try manager.writeIntents(id, &.{}, &.{});
    const contract = try manager.preparedEffectsContract(id, @splat(73), .synchronous, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try manager.sealPreparedEffects(id, frame, @splat(73), .synchronous, null);
    var record = try manager.loadTransactionRecord(id);
    try applyResolveDecision(&record, .committed, 2);
    try manager.saveTransactionRecord(makeRecordKey(id), record);
    var plan = (try manager.loadPreparedEffects(a, id, null)).?;
    defer plan.deinit();
    const batch: ResolutionExtraBatch = .{ .writes = &.{.{ .key = "accepted-artifact", .value = "accepted" }} };
    try std.testing.expect((try manager.resolvePreparedIntents(id, 2, &plan, batch, null)).applied);
    const value = try manager.getAllocWithAdmission(a, "accepted-artifact", null);
    defer a.free(value);
    try std.testing.expectEqualStrings("accepted", value);
    try std.testing.expect(!try manager.hasPreparedEffects(id));
    try std.testing.expect(!try manager.hasPreparationRequirement(id));
    try manager.applyBatch(&.{.{ .key = "accepted-artifact", .value = "newer" }}, &.{}, null);
    try std.testing.expect(!(try manager.resolvePreparedIntents(id, 999, &plan, batch, null)).applied);
    const newer = try manager.getAllocWithAdmission(a, "accepted-artifact", null);
    defer a.free(newer);
    try std.testing.expectEqualStrings("newer", newer);
}

test "prepared transaction effects resolution status preserves original admission errors" {
    const Control = struct {
        count: usize = 0,
        reject: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            if (self.count == self.reject) return self.cause;
        }
    };
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(0xd6);
    try manager.initTransaction(id, 1);
    var c: Control = .{};
    const payload: read_control.Payload = .{ .ptr = &c, .vtable = &.{ .checkpoint = Control.check } };
    const port = read_control.Port.init(&payload);
    for ([_]TxnId{ id, @splat(0xd7) }) |lookup| {
        c = .{};
        const baseline = manager.getTransactionStatusWithAdmission(lookup, port);
        if (std.mem.eql(u8, &lookup, &id)) {
            try std.testing.expectEqual(TxnStatus.pending, try baseline);
        } else try std.testing.expectError(TxnError.TxnNotFound, baseline);
        const count = c.count;
        try std.testing.expect(count > 0);
        for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.MemoryLimitExceeded }) |cause| {
            for (1..count + 1) |reject| {
                c = .{ .reject = reject, .cause = cause };
                try std.testing.expectError(cause, manager.getTransactionStatusWithAdmission(lookup, port));
                try std.testing.expectEqual(reject, c.count);
            }
        }
    }
}

test "prepared transaction effects catalog lease fences changes and permits identical publication" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const inventory = @import("db/artifact_inventory.zig");
    const topology = @import("db/relational_integrity_topology.zig");
    const key = makePreparedEffectsKey(@splat(0xe1));
    try manager.applyBatch(&.{.{ .key = inventory.index_key, .value = "original" }}, &.{}, null);
    {
        var writer = try manager.store.beginWrite();
        errdefer writer.abort();
        try inventory.requirePreparedCatalogUnchanged(&writer, inventory.index_key, "before-seal");
        // Even a malformed/orphan frame pins the catalog and namespace. No
        // frame payload is decoded to establish the lease.
        try writer.put(&key, "opaque-orphan");
        try std.testing.expect(try inventory.hasPreparedCatalogLease(&writer));
        try std.testing.expectError(error.SchemaInUse, inventory.requirePreparedCatalogUnchanged(&writer, inventory.index_key, "changed"));
        try inventory.requirePreparedCatalogUnchanged(&writer, inventory.index_key, "original");
        try std.testing.expectError(error.SchemaInUse, topology.requireUnfencedOrUnchanged(&writer, inventory.enrichment_key, "new"));
        try inventory.requirePreparedCatalogUnchanged(&writer, @import("db/catalog/resolver_catalog.zig").merge_catalog_key, "must-drain");
        try writer.commit();
    }
    try std.testing.expect(try manager.hasSchemaLeases());
    {
        var writer = try manager.store.beginWrite();
        errdefer writer.abort();
        try std.testing.expectError(error.SchemaInUse, inventory.requirePreparedCatalogUnchanged(&writer, inventory.resolver_key, "new"));
        try writer.delete(&key);
        try std.testing.expect(!try inventory.hasPreparedCatalogLease(&writer));
        try inventory.requirePreparedCatalogUnchanged(&writer, inventory.index_key, "after-retirement");
        try writer.put(inventory.index_key, "after-retirement");
        try writer.commit();
    }
    try std.testing.expect(!try manager.hasSchemaLeases());
}

test "prepared transaction effects retained contract never upgrades legacy or accepts damaged evidence" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const refused: TxnId = @splat(0xe0);
    const refused_key = makeRecordKey(refused);
    try std.testing.expectError(error.InvalidPreparationRequirement, manager.initTransactionRequiringPreparedEffects(refused, 1, 1, &.{}, false, true, .{ .mode = .synchronous }, .{ .writes = &.{.{ .key = &refused_key, .value = "override" }} }, null));
    try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(refused));
    const id: TxnId = @splat(0xe1);
    try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, true, .{ .mode = .synchronous }, .{}, null);
    try std.testing.expectEqual(prepared_effects.Mode.synchronous, (try manager.getPreparationDecision(id, null)).requirement.?.mode);
    const requirement_key = makePreparationRequirementKey(id);
    try manager.applyBatch(&.{}, &.{&requirement_key}, null);
    try std.testing.expectError(error.PreparedEffectsRequired, manager.getPreparationDecision(id, null));
    try std.testing.expectError(error.PreparedEffectsRequired, manager.resolveIntents(id, .committed, 10));
    const wrong = (prepared_effects.Requirement{ .mode = .deferred }).encode();
    try manager.applyBatch(&.{.{ .key = &requirement_key, .value = &wrong }}, &.{}, null);
    try std.testing.expectError(error.PreparationContractConflict, manager.getPreparationDecision(id, null));
    try manager.resolveIntents(id, .aborted, 10);
    try std.testing.expectEqual(TxnStatus.aborted, (try manager.getPreparationDecision(id, null)).status);
    const key = makeRecordKey(id);
    const raw = try manager.getAlloc(a, &key);
    defer a.free(raw);
    try std.testing.expectEqual(txn_record_v7_size, raw.len);
    for ([_]u8{ 0, 3, 255 }) |tag| {
        raw[53] = tag;
        try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(raw));
    }
    // Previous readers reject this longer record. Conversely, new readers
    // never infer a contract from a v6 record, even with a valid sidecar.
    var legacy = try decodeRecord(raw[0..txn_record_v6_size]);
    try std.testing.expect(legacy.preparation_requirement == null);
    legacy.status = .pending;
    legacy.intents_resolved = false;
    try manager.saveTransactionRecord(key, legacy);
    try manager.applyBatch(&.{.{ .key = &requirement_key, .value = &wrong }}, &.{}, null);
    try std.testing.expect((try manager.getPreparationDecision(id, null)).requirement == null);
    try std.testing.expectError(error.PreparationContractConflict, manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, true, .{ .mode = .deferred }, .{}, null));
}

test "prepared transaction effects retained decision preserves original admission rejections" {
    const Control = struct {
        count: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            if (self.count == self.fail_at) return self.cause;
        }
    };
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(0xe2);
    try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, true, .{ .mode = .deferred }, .{}, null);
    var owner: Control = .{};
    const payload: read_control.Payload = .{ .ptr = &owner, .vtable = &.{ .checkpoint = Control.check } };
    _ = try manager.getPreparationDecision(id, read_control.Port.init(&payload));
    const checkpoints = owner.count;
    try std.testing.expect(checkpoints > 0);
    for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.Canceled, error.MemoryLimitExceeded }) |cause| {
        for (1..checkpoints + 1) |fail_at| {
            owner = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, manager.getPreparationDecision(id, read_control.Port.init(&payload)));
        }
    }
}

test "prepared transaction effects ordered seal publishes frame grant and receipt atomically at every allocation" {
    const Case = struct {
        fn run(failing: Allocator) !void {
            const a = std.testing.allocator;
            var backend = mem_backend.Backend.init(a, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(a, .{});
            defer runtime.deinit();
            var manager = try TxnManager.init(a, &runtime);
            defer manager.deinit();
            const id: TxnId = @splat(0xe6);
            try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, true, .{ .mode = .synchronous }, .{}, null);
            try manager.writeIntents(id, &.{}, &.{});
            const contract = try manager.preparedEffectsContract(id, @splat(1), .synchronous, null);
            const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
            defer a.free(frame);
            manager.alloc = failing;
            const result = manager.sealPreparedEffectsWithExtraBatch(id, frame, @splat(1), .synchronous, .{ .writes = &.{.{ .key = "ordered-seal-receipt", .value = "accepted" }} }, null);
            manager.alloc = a;
            if (result) |_| {
                try std.testing.expect(try manager.hasPreparedEffects(id));
                const value = try manager.getAlloc(a, "ordered-seal-receipt");
                defer a.free(value);
                try std.testing.expectEqualStrings("accepted", value);
            } else |err| {
                try std.testing.expect(!try manager.hasPreparedEffects(id));
                try std.testing.expectError(error.NotFound, manager.getAlloc(a, "ordered-seal-receipt"));
                return err;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prepared transaction effects bound begin is atomic at every allocation and never upgrades legacy" {
    const Case = struct {
        fn run(failing: Allocator) !void {
            const a = std.testing.allocator;
            for (std.meta.tags(prepared_effects.Mode)) |mode| {
                var backend = mem_backend.Backend.init(a, .{});
                defer backend.close();
                var runtime = try backend.runtimeStore(a, .{});
                defer runtime.deinit();
                var manager = try TxnManager.init(a, &runtime);
                defer manager.deinit();
                const binding: prepared_effects.Binding = .{ .group_id = 7, .topology_epoch = 3, .participant_digest = @splat(1), .route_digest = @splat(2) };
                const legacy: TxnId = @splat(0xe7);
                try manager.initTransactionRequiringPreparedEffects(legacy, 1, 1, &.{}, false, true, .{ .mode = mode }, .{}, null);
                try std.testing.expectError(error.PreparationContractConflict, manager.initTransactionRequiringPreparedEffectsWithBinding(legacy, 1, 1, &.{}, false, true, .{ .mode = mode }, binding, .{}, null));
                try std.testing.expect((try manager.getPreparationDecision(legacy, null)).binding == null);
                const id: TxnId = @splat(0xe8);
                manager.alloc = failing;
                const result = manager.initTransactionRequiringPreparedEffectsWithBinding(id, 1, 1, &.{}, false, true, .{ .mode = mode }, binding, .{ .writes = &.{.{ .key = "bound-begin-receipt", .value = "accepted" }} }, null);
                manager.alloc = a;
                if (result) |_| {
                    try std.testing.expectEqualDeep(binding, (try manager.getPreparationDecision(id, null)).binding.?);
                    const key = makeRecordKey(id);
                    const raw = try manager.getAlloc(a, &key);
                    defer a.free(raw);
                    try std.testing.expectEqual(txn_record_v8_size, raw.len);
                    try std.testing.expect((try decodeRecord(raw[0..txn_record_v7_size])).preparation_binding == null);
                    try std.testing.expectError(error.PreparationContractConflict, manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, true, .{ .mode = mode }, .{}, null));
                    try manager.resolveIntents(id, .aborted, 10);
                    try std.testing.expectEqualDeep(binding, (try manager.getPreparationDecision(id, null)).binding.?);
                    try std.testing.expect(try manager.cleanupTransactionMetadataIfEligible(id, 100, 100));
                    try std.testing.expectError(error.TxnNotFound, manager.getPreparationDecision(id, null));
                } else |err| {
                    try std.testing.expectError(error.TxnNotFound, manager.getPreparationDecision(id, null));
                    try std.testing.expect(!try manager.hasPreparationRequirement(id));
                    try std.testing.expectError(error.NotFound, manager.getAlloc(a, "bound-begin-receipt"));
                    return err;
                }
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prepared transaction effects bound receipt preserves every original rejection" {
    const Control = struct {
        count: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            if (self.count == self.fail_at) return self.cause;
        }
    };
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(0xe9);
    const binding: prepared_effects.Binding = .{ .group_id = 7, .topology_epoch = 3, .participant_digest = @splat(1), .route_digest = @splat(2) };
    try manager.initTransactionRequiringPreparedEffectsWithBinding(id, 1, 1, &.{}, false, true, .{ .mode = .synchronous }, binding, .{}, null);
    try manager.writeIntents(id, &.{}, &.{});
    const contract = try manager.preparedEffectsContract(id, @splat(3), .synchronous, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try manager.sealBoundPreparedEffectsWithExtraBatch(id, frame, @splat(3), .synchronous, binding, .{}, null);
    var control: Control = .{};
    const payload: read_control.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check } };
    _ = try manager.getBoundPreparedSealReceipt(id, binding, .init(&payload));
    const checkpoints = control.count;
    try std.testing.expect(checkpoints > 0);
    for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.Canceled, error.MemoryLimitExceeded }) |cause| {
        for (1..checkpoints + 1) |fail_at| {
            control = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, manager.getBoundPreparedSealReceipt(id, binding, .init(&payload)));
        }
    }
}

test "prepared transaction effects snapshot retains exactly the hashed producer inputs" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(151);
    const catalog: [32]u8 = @splat(39);
    try manager.initTransaction(id, 17);
    try manager.writeIntentsExtraBatch(id, &.{
        .{ .key = "z-row", .value = "original", .prepared_row = "typed-row" },
        .{ .key = "a-delete", .value = null },
    }, &.{}, .{ .schema_binding = .{ .version = 23 } });
    var snapshot = try manager.capturePreparedIntents(a, id, catalog, .deferred, null);
    defer snapshot.deinit();
    try std.testing.expectEqualDeep(try manager.preparedEffectsContract(id, catalog, .deferred, null), snapshot.contract);
    try std.testing.expectEqual(snapshot.batch.revision, snapshot.contract.intent_revision);
    try std.testing.expectEqual(@as(?u32, 23), snapshot.contract.schema_version);
    try std.testing.expectEqualStrings("z-row", snapshot.batch.writes[0].key);
    try std.testing.expectEqualStrings("original", snapshot.batch.writes[0].value);
    try std.testing.expectEqualStrings("typed-row", snapshot.batch.prepared_rows[0].?);
    try std.testing.expectEqualStrings("a-delete", snapshot.batch.deletes[0]);
    const entry = snapshot.batch.owned_entries.?[1];
    const decoded = try IntentValue.decode(entry.value);
    try std.testing.expectEqual(decoded.value.?.ptr, snapshot.batch.writes[0].value.ptr);
    try std.testing.expectEqual(decoded.prepared_row.?.ptr, snapshot.batch.prepared_rows[0].?.ptr);
    // A later prepare cannot mutate the bytes used by an in-flight producer.
    try manager.writeIntentsExtraBatch(id, &.{.{ .key = "z-row", .value = "replacement", .prepared_row = "new-row" }}, &.{}, .{ .schema_binding = .{ .version = 23 } });
    try std.testing.expectEqualStrings("original", snapshot.batch.writes[0].value);
    try std.testing.expectEqualStrings("typed-row", snapshot.batch.prepared_rows[0].?);
    const stale = try prepared_effects.encode(a, .{ .contract = snapshot.contract, .effects = .{} }, null);
    defer a.free(stale);
    try std.testing.expectError(error.PreparedEffectsMismatch, manager.sealPreparedEffects(id, stale, catalog, .deferred, null));
    try std.testing.expect(!try manager.hasPreparedEffects(id));
    var current = try manager.capturePreparedIntents(a, id, catalog, .deferred, null);
    defer current.deinit();
    try std.testing.expect(!std.mem.eql(u8, &snapshot.contract.intent_digest, &current.contract.intent_digest));
    const frame = try prepared_effects.encode(a, .{ .contract = current.contract, .effects = .{} }, null);
    defer a.free(frame);
    try manager.sealPreparedEffects(id, frame, catalog, .deferred, null);
    try std.testing.expectError(error.PreparedEffectsSealed, manager.capturePreparedIntents(a, id, catalog, .deferred, null));
    // Opaque/corrupt frame bytes are still accepted-work metadata, never an
    // invitation to run the producer again. No frame decode is needed here.
    const sealed_key = makePreparedEffectsKey(id);
    try manager.applyBatch(&.{.{ .key = &sealed_key, .value = "opaque" }}, &.{}, null);
    try std.testing.expectError(error.PreparedEffectsSealed, manager.capturePreparedIntents(a, id, catalog, .deferred, null));
}

test "prepared transaction effects snapshot owns allocation refusal and every original checkpoint" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(152);
    try manager.initTransaction(id, 18);
    const value = try a.alloc(u8, 13000);
    defer a.free(value);
    @memset(value, 'v');
    try manager.writeIntents(id, &.{.{ .key = "row", .value = value }}, &.{});
    const Checks = struct {
        count: usize = 0,
        stop: usize = std.math.maxInt(usize),
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
            if (self.count >= self.stop) return error.DeadlineExceeded;
        }
        fn capture(alloc: Allocator, mgr: *TxnManager, txn_id: TxnId) !void {
            var owned = try mgr.capturePreparedIntents(alloc, txn_id, @splat(1), .deferred, null);
            defer owned.deinit();
            try std.testing.expectEqual(@as(usize, 13000), owned.batch.writes[0].value.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Checks.capture, .{ &manager, id });
    var checks: Checks = .{};
    var payload: read_control.Payload = .{ .ptr = &checks, .vtable = &.{ .checkpoint = Checks.check } };
    var complete = try manager.capturePreparedIntents(a, id, @splat(1), .deferred, read_control.Port.init(&payload));
    const expected = complete.contract;
    complete.deinit();
    const checkpoints = checks.count;
    try std.testing.expect(checkpoints > 8);
    for (1..checkpoints + 1) |stop| {
        checks = .{ .stop = stop };
        try std.testing.expectError(error.DeadlineExceeded, manager.capturePreparedIntents(a, id, @splat(1), .deferred, read_control.Port.init(&payload)));
        try std.testing.expectEqual(stop, checks.count);
        try std.testing.expect(!try manager.hasPreparedEffects(id));
    }
    var retried = try manager.capturePreparedIntents(a, id, @splat(1), .deferred, null);
    defer retried.deinit();
    try std.testing.expectEqualDeep(expected, retried.contract);
}

test "prepared transaction effects snapshot refuses terminal decisions and conflicting required modes" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(153);
    try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
    var no_allocations = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.PreparationContractConflict, manager.capturePreparedIntents(no_allocations.allocator(), id, @splat(2), .deferred, null));
    var record = try manager.loadTransactionRecord(id);
    record.status = .committed;
    record.commit_version = 2;
    try manager.saveTransactionRecord(makeRecordKey(id), record);
    try std.testing.expectError(error.DecisionConflict, manager.capturePreparedIntents(no_allocations.allocator(), id, @splat(2), .synchronous, null));
    // A durable COMMIT awaiting apply still needs its existing recovery digest.
    _ = try manager.preparedEffectsContract(id, @splat(2), .synchronous, null);
    record.status = .aborted;
    try manager.saveTransactionRecord(makeRecordKey(id), record);
    try std.testing.expectError(error.DecisionConflict, manager.capturePreparedIntents(no_allocations.allocator(), id, @splat(2), .synchronous, null));
}

test "prepared transaction effects paired resolution retains validated inputs after backend close" {
    const a = std.testing.allocator;
    const id: TxnId = @splat(154);
    var captured: PreparedResolutionInputs = blk: {
        var backend = mem_backend.Backend.init(a, .{});
        defer backend.close();
        var runtime = try backend.runtimeStore(a, .{});
        defer runtime.deinit();
        var manager = try TxnManager.init(a, &runtime);
        defer manager.deinit();
        try manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, false, .{ .mode = .synchronous }, .{}, null);
        try manager.writeIntentsExtraBatch(id, &.{
            .{ .key = "row", .value = "original", .prepared_row = "schema-bound-row" },
            .{ .key = "removed", .value = null },
        }, &.{}, .{ .schema_binding = .{ .version = 21 } });
        const contract = try manager.preparedEffectsContract(id, @splat(4), .synchronous, null);
        const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{
            .artifact_writes = @constCast(&[_]@import("db/types.zig").BatchWrite{.{ .key = "accepted-key", .value = "accepted-value" }}),
        } }, null);
        defer a.free(frame);
        try manager.sealPreparedEffects(id, frame, @splat(4), .synchronous, null);
        // Recovery after the decision still owns the original inputs; it must
        // not use the producer-only capture which rejects terminal decisions.
        var record = try manager.loadTransactionRecord(id);
        record.status = .committed;
        record.commit_version = 2;
        try manager.saveTransactionRecord(makeRecordKey(id), record);
        const owned = (try manager.loadPreparedEffectsWithInputs(a, id, null)).?;
        try std.testing.expectEqualDeep(contract, owned.inputs.contract);
        try std.testing.expectEqualDeep(contract, owned.effects.decoded.plan.contract);
        break :blk owned;
    };
    defer captured.deinit();
    try std.testing.expectEqualStrings("original", captured.inputs.batch.writes[0].value);
    try std.testing.expectEqualStrings("schema-bound-row", captured.inputs.batch.prepared_rows[0].?);
    try std.testing.expectEqualStrings("removed", captured.inputs.batch.deletes[0]);
    try std.testing.expectEqualStrings("accepted-value", captured.effects.decoded.plan.effects.artifact_writes[0].value);
}

test "prepared transaction effects paired resolution unwinds allocations and every original rejection" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(155);
    try manager.initTransaction(id, 1);
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "original" }}, &.{});
    const contract = try manager.preparedEffectsContract(id, @splat(4), .deferred, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try manager.sealPreparedEffects(id, frame, @splat(4), .deferred, null);
    const Checks = struct {
        count: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.DeadlineExceeded,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            if (self.count == self.stop) return self.cause;
        }
        fn capture(alloc: Allocator, mgr: *TxnManager, txn_id: TxnId) !void {
            var owned = (try mgr.loadPreparedEffectsWithInputs(alloc, txn_id, null)).?;
            defer owned.deinit();
            try std.testing.expectEqualDeep(owned.inputs.contract, owned.effects.decoded.plan.contract);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Checks.capture, .{ &manager, id });
    var checks: Checks = .{};
    var payload: read_control.Payload = .{ .ptr = &checks, .vtable = &.{ .checkpoint = Checks.check } };
    var complete = (try manager.loadPreparedEffectsWithInputs(a, id, read_control.Port.init(&payload))).?;
    complete.deinit();
    const checkpoints = checks.count;
    try std.testing.expect(checkpoints > 8);
    for ([_]anyerror{ error.DeadlineExceeded, error.NotFound }) |cause| {
        for (1..checkpoints + 1) |stop| {
            checks = .{ .stop = stop, .cause = cause };
            try std.testing.expectError(cause, manager.loadPreparedEffectsWithInputs(a, id, read_control.Port.init(&payload)));
            try std.testing.expectEqual(stop, checks.count);
        }
    }
    try Checks.capture(a, &manager, id);
    // The decoder must reject an intent contract mismatch rather than pair a
    // newly collected revision with the old accepted effect slab.
    var record = try manager.loadTransactionRecord(id);
    record.intent_revision += 1;
    try manager.saveTransactionRecord(makeRecordKey(id), record);
    try std.testing.expectError(error.PreparedEffectsMismatch, manager.loadPreparedEffectsWithInputs(a, id, null));
    try std.testing.expect(try manager.hasPreparedEffects(id));
}

const PreparedReplaySealFixture = struct {
    native: lsm_backend.Backend = undefined,
    store: DocStore = undefined,
    manager: TxnManager = undefined,
    const id: TxnId = @splat(156);
    const catalog: [32]u8 = @splat(5);
    const credit = @import("prepared_replay_credit.zig");
    const backlog = @import("publication_backlog.zig");
    fn init(self: *@This()) !void {
        const a = std.testing.allocator;
        self.native = lsm_backend.Backend.init(a, .{});
        errdefer self.native.close();
        self.store = try DocStore.openRuntime(a, try self.native.runtimeStore(a, .{}));
        errdefer self.store.close();
        try @import("artifact_footprint.zig").initializeEmpty(&self.store);
        try @import("artifact_replay_inventory.zig").initialize(&self.store);
        try self.store.put(@import("db/catalog/reader_versions.zig").minimum_reader_key, "11");
        try self.store.put(@import("db/catalog/reader_versions.zig").replacement_reader_base_key, "11");
        self.manager = try TxnManager.init(a, &self.store);
        errdefer self.manager.deinit();
        try self.manager.initTransactionRequiringPreparedEffects(id, 1, 1, &.{}, false, false, .{ .mode = .deferred }, .{}, null);
        try self.manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
    }
    fn deinit(self: *@This()) void {
        self.manager.deinit();
        self.store.close();
        self.native.close();
    }
    fn frame(self: *@This()) ![]u8 {
        const contract = try self.manager.preparedEffectsContract(id, catalog, .deferred, null);
        return prepared_effects.encode(std.testing.allocator, .{ .contract = contract, .effects = .{} }, null);
    }
    fn seal(self: *@This(), raw: []const u8, key: []const u8, receipt: []const u8, original: ?read_control.Port) !void {
        return self.manager.sealPreparedEffectsAndReplayPlan(id, raw, catalog, .deferred, null, .{ .documents = &.{.{ .key = key }} }, .{ .writes = &.{.{ .key = "seal-receipt", .value = receipt }} }, original);
    }
    fn debt(self: *@This()) !backlog.State {
        var read = try self.store.beginReadTxn();
        defer read.abort();
        return (try backlog.load(&read)) orelse .{};
    }
    fn noSeal(self: *@This()) !void {
        const a = std.testing.allocator;
        try std.testing.expect(!try self.manager.hasPreparedEffects(id));
        try std.testing.expectError(error.NotFound, self.store.get(a, "seal-receipt"));
        var read = try self.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try credit.load(&read, id)) == null);
    }
};

test "prepared transaction effects replay seal publishes frame grant and receipt atomically" {
    const a = std.testing.allocator;
    const F = PreparedReplaySealFixture;
    var f: F = .{};
    try f.init();
    defer f.deinit();
    const frame = try f.frame();
    defer a.free(frame);
    try f.seal(frame, "row", "accepted", null);
    const debt = try f.debt();
    {
        var read = try f.store.beginReadTxn();
        defer read.abort();
        const grant = (try F.credit.load(&read, F.id)).?;
        try std.testing.expectEqual(F.credit.Phase.prepared, grant.phase);
        try std.testing.expectEqual(@as(u64, 1), grant.root_count);
        try std.testing.expectEqualDeep((try prepared_effects.inspect(frame, null)).fingerprint, grant.frame);
    }
    try f.seal(frame, "row", "accepted", null);
    try std.testing.expectEqualDeep(debt, try f.debt());
    try std.testing.expectError(error.PreparedEffectsConflict, f.seal(frame, "different-root", "replaced", null));
    const receipt = try f.store.get(a, "seal-receipt");
    defer a.free(receipt);
    try std.testing.expectEqualStrings("accepted", receipt);
    try std.testing.expectEqualDeep(debt, try f.debt());
    try f.manager.resolveIntents(F.id, .aborted, 2);
    try std.testing.expect(!try f.manager.hasPreparedEffects(F.id));
    var read = try f.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect((try F.credit.load(&read, F.id)) == null);
    try std.testing.expectEqual(@as(u64, 0), (try f.debt()).bytes);
}

test "prepared transaction effects replay seal refuses exhausted capacity without freezing inputs" {
    const a = std.testing.allocator;
    const F = PreparedReplaySealFixture;
    var f: F = .{};
    try f.init();
    defer f.deinit();
    const frame = try f.frame();
    defer a.free(frame);
    const initial = try f.debt();
    const filler = F.backlog.transactionCreditKey(@splat(254));
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, F.backlog.default_max_bytes - initial.bytes - filler.len, .little);
    try f.store.put(&filler, &raw);
    try std.testing.expectError(error.Backpressured, f.seal(frame, "row", "refused", null));
    try f.noSeal();
    try f.manager.writeIntents(F.id, &.{.{ .key = "row", .value = "[]" }}, &.{});
    try f.store.delete(&filler);
    const retry = try f.frame();
    defer a.free(retry);
    try f.seal(retry, "row", "accepted", null);
}

test "prepared transaction effects replay seal rolls back each allocation and original rejection" {
    const F = PreparedReplaySealFixture;
    const Checks = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.DeadlineExceeded;
        }
        fn run(allocator: Allocator, checks: ?*@This()) !void {
            const a = std.testing.allocator;
            var f: F = .{};
            try f.init();
            defer f.deinit();
            const frame = try f.frame();
            defer a.free(frame);
            var payload: read_control.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            f.manager.alloc = allocator;
            const outcome = f.seal(frame, "row", "accepted", if (checks != null) read_control.Port.init(&payload) else null);
            f.manager.alloc = a;
            if (outcome) |_| {
                try std.testing.expect(try f.manager.hasPreparedEffects(F.id));
                var read = try f.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect((try F.credit.load(&read, F.id)) != null);
            } else |err| {
                try f.noSeal();
                return err;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    try std.testing.expect(baseline.calls > 10);
    for (1..baseline.calls + 1) |fail_at| {
        var checks: Checks = .{ .fail_at = fail_at };
        const outcome = Checks.run(std.testing.allocator, &checks);
        if (checks.calls >= fail_at) try std.testing.expectError(error.DeadlineExceeded, outcome) else try outcome;
    }
}

test "prepared transaction effects replay seal fails closed on unsupported raw backends" {
    const a = std.testing.allocator;
    var native = mem_backend.Backend.init(a, .{});
    defer native.close();
    var runtime = try native.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(157);
    try manager.initTransaction(id, 1);
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
    const contract = try manager.preparedEffectsContract(id, @splat(5), .deferred, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try std.testing.expectError(error.Unsupported, manager.sealPreparedEffectsAndReplayPlan(id, frame, @splat(5), .deferred, null, .{}, .{}, null));
    try std.testing.expect(!try manager.hasPreparedEffects(id));
}

test "prepared transaction effects replay abort preserves grant and frame on every refusal" {
    const F = PreparedReplaySealFixture;
    const Checks = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.DeadlineExceeded;
        }
        fn run(allocator: Allocator, checks: ?*@This()) !void {
            const a = std.testing.allocator;
            var f: F = .{};
            try f.init();
            defer f.deinit();
            const frame = try f.frame();
            defer a.free(frame);
            try f.seal(frame, "row", "accepted", null);
            const debt = try f.debt();
            var payload: read_control.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            f.manager.alloc = allocator;
            const outcome = f.manager.resolveIntentsWithPreparedEffects(F.id, .aborted, 2, .{}, null, if (checks != null) read_control.Port.init(&payload) else null);
            f.manager.alloc = a;
            if (outcome) |_| {
                try std.testing.expect(!try f.manager.hasPreparedEffects(F.id));
                var read = try f.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect((try F.credit.load(&read, F.id)) == null);
                try std.testing.expectEqual(@as(u64, 0), (try f.debt()).bytes);
            } else |err| {
                try std.testing.expect(try f.manager.hasPreparedEffects(F.id));
                try std.testing.expectEqualDeep(debt, try f.debt());
                var read = try f.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect((try F.credit.load(&read, F.id)) != null);
                return err;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    try std.testing.expect(baseline.calls > 10);
    for (1..baseline.calls + 1) |fail_at| {
        var checks: Checks = .{ .fail_at = fail_at };
        const outcome = Checks.run(std.testing.allocator, &checks);
        if (checks.calls >= fail_at) try std.testing.expectError(error.DeadlineExceeded, outcome) else try outcome;
    }
}

test "prepared transaction effects replay commit consumes reserved capacity and acknowledgements remain independent" {
    const a = std.testing.allocator;
    const F = PreparedReplaySealFixture;
    for ([_]bool{ false, true }) |replay_first| {
        var f: F = .{};
        try f.init();
        defer f.deinit();
        const frame = try f.frame();
        defer a.free(frame);
        try f.seal(frame, "row", "accepted", null);
        var accepted = (try f.manager.loadPreparedEffects(a, F.id, null)).?;
        defer accepted.deinit();
        const journal = @import("db/derived/change_journal.zig");
        const payload = try journal.encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = &.{"row"} });
        defer a.free(payload);
        const batch_key = makeTransactionReplicationBatchOutboxKey(F.id);
        const before = try f.debt();
        const filler = F.backlog.transactionCreditKey(@splat(254));
        var fill: [8]u8 = undefined;
        std.mem.writeInt(u64, &fill, F.backlog.default_max_bytes - before.bytes - filler.len, .little);
        try f.store.put(&filler, &fill);
        try std.testing.expect((try f.manager.resolvePreparedIntents(F.id, 2, &accepted, .{
            .writes = &.{.{ .key = &batch_key, .value = "batch" }},
            .replay = .{ .sequence = 1, .payload = payload },
        }, null)).applied);
        try std.testing.expect(!try f.manager.hasPreparedEffects(F.id));
        {
            var read = try f.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(F.credit.Phase.materialized, (try F.credit.load(&read, F.id)).?.phase);
        }
        var outbox = try f.manager.loadReplicationOutbox(a, F.id);
        defer outbox.deinit(a);
        try std.testing.expectEqualStrings("batch", outbox.batch_payload.?);
        try std.testing.expectEqualSlices(u8, payload, outbox.replay_payload.?);
        // A terminal retry neither rebuilds the outbox nor applies the old row.
        try std.testing.expect(!(try f.manager.resolvePreparedIntents(F.id, 999, &accepted, .{}, null)).applied);
        try f.manager.clearReplicationOutbox(F.id, if (replay_first) .replay else .batch);
        {
            var read = try f.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(!replay_first, (try F.credit.load(&read, F.id)) != null);
        }
        var remaining = try f.manager.loadReplicationOutbox(a, F.id);
        defer remaining.deinit(a);
        try std.testing.expectEqual(!replay_first, remaining.replay_payload != null);
        try std.testing.expectEqual(replay_first, remaining.batch_payload != null);
        try f.manager.clearReplicationOutbox(F.id, if (replay_first) .batch else .replay);
        try f.store.delete(&filler);
        try std.testing.expectEqual(@as(u64, 0), (try f.debt()).bytes);
    }
}

test "prepared transaction effects replay commit refuses missing and oversized replay without retiring its frame" {
    const a = std.testing.allocator;
    const F = PreparedReplaySealFixture;
    var f: F = .{};
    try f.init();
    defer f.deinit();
    const frame = try f.frame();
    defer a.free(frame);
    try f.seal(frame, "row", "accepted", null);
    var accepted = (try f.manager.loadPreparedEffects(a, F.id, null)).?;
    defer accepted.deinit();
    try std.testing.expectError(error.PreparedEffectsRequired, f.manager.resolvePreparedIntents(F.id, 2, &accepted, .{}, null));
    const journal = @import("db/derived/change_journal.zig");
    const oversized_key: [4096]u8 = @splat('x');
    const oversized = try journal.encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = &.{&oversized_key} });
    defer a.free(oversized);
    try std.testing.expectError(error.InvalidHAOutbox, f.manager.resolvePreparedIntents(F.id, 2, &accepted, .{ .replay = .{ .sequence = 1, .payload = oversized } }, null));
    try std.testing.expect(try f.manager.hasPreparedEffects(F.id));
    try std.testing.expectEqual(TxnStatus.pending, try f.manager.getTransactionStatus(F.id));
    var read = try f.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(F.credit.Phase.prepared, (try F.credit.load(&read, F.id)).?.phase);
    var outbox = try f.manager.loadReplicationOutbox(a, F.id);
    defer outbox.deinit(a);
    try std.testing.expect(outbox.replay_payload == null);
}

test "prepared transaction effects replay commit unwinds every allocation and original checkpoint" {
    const F = PreparedReplaySealFixture;
    const Checks = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.DeadlineExceeded;
        }
        fn run(allocator: Allocator, checks: ?*@This()) !void {
            const a = std.testing.allocator;
            var f: F = .{};
            try f.init();
            defer f.deinit();
            const frame = try f.frame();
            defer a.free(frame);
            try f.seal(frame, "row", "accepted", null);
            var accepted = (try f.manager.loadPreparedEffects(a, F.id, null)).?;
            defer accepted.deinit();
            const payload = try @import("db/derived/change_journal.zig").encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = &.{"row"} });
            defer a.free(payload);
            const debt = try f.debt();
            var control: read_control.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            f.manager.alloc = allocator;
            const outcome = f.manager.resolvePreparedIntents(F.id, 2, &accepted, .{ .replay = .{ .sequence = 1, .payload = payload } }, if (checks != null) read_control.Port.init(&control) else null);
            f.manager.alloc = a;
            if (outcome) |_| {
                try std.testing.expect(!try f.manager.hasPreparedEffects(F.id));
                var read = try f.store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqual(F.credit.Phase.materialized, (try F.credit.load(&read, F.id)).?.phase);
            } else |err| {
                try std.testing.expect(try f.manager.hasPreparedEffects(F.id));
                try std.testing.expectEqualDeep(debt, try f.debt());
                try std.testing.expectEqual(TxnStatus.pending, try f.manager.getTransactionStatus(F.id));
                var outbox = try f.manager.loadReplicationOutbox(a, F.id);
                defer outbox.deinit(a);
                try std.testing.expect(outbox.replay_payload == null);
                return err;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    try std.testing.expect(baseline.calls > 10);
    for (1..baseline.calls + 1) |fail_at| {
        var checks: Checks = .{ .fail_at = fail_at };
        const outcome = Checks.run(std.testing.allocator, &checks);
        if (checks.calls >= fail_at) try std.testing.expectError(error.DeadlineExceeded, outcome) else try outcome;
    }
}

test "prepared transaction effects replay commit rejects grant metadata on unsupported raw backends" {
    const a = std.testing.allocator;
    var native = mem_backend.Backend.init(a, .{});
    defer native.close();
    var runtime = try native.runtimeStore(a, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(a, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(158);
    try manager.initTransaction(id, 1);
    try manager.writeIntents(id, &.{.{ .key = "row", .value = "{}" }}, &.{});
    const contract = try manager.preparedEffectsContract(id, @splat(5), .deferred, null);
    const frame = try prepared_effects.encode(a, .{ .contract = contract, .effects = .{} }, null);
    defer a.free(frame);
    try manager.sealPreparedEffects(id, frame, @splat(5), .deferred, null);
    var accepted = (try manager.loadPreparedEffects(a, id, null)).?;
    defer accepted.deinit();
    // Raw imported bytes do not authorize this adapter to serve funded work.
    const credit = @import("prepared_replay_credit.zig");
    const grant: credit.Grant = .{ .owner = id, .frame = accepted.fingerprint, .roots = @splat(0), .root_count = 0, .base = .{}, .current = .{} };
    try manager.applyBatch(&.{.{ .key = &credit.grantKey(id), .value = &try grant.encode() }}, &.{}, null);
    const payload = try @import("db/derived/change_journal.zig").encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = &.{"row"} });
    defer a.free(payload);
    try std.testing.expectError(error.Unsupported, manager.resolvePreparedIntents(id, 2, &accepted, .{ .replay = .{ .sequence = 1, .payload = payload } }, null));
    try std.testing.expect(try manager.hasPreparedEffects(id));
    try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(id));
    var outbox = try manager.loadReplicationOutbox(a, id);
    defer outbox.deinit(a);
    try std.testing.expect(outbox.replay_payload == null);
}

test "prepared transaction effects replay seal derives primary floors from exact stored intents" {
    const a = std.testing.allocator;
    const F = PreparedReplaySealFixture;
    var f: F = .{};
    try f.init();
    defer f.deinit();
    // Include a private physical metadata intent. It is not a logical document
    // and must not acquire a phantom primary root or replay-key allowance.
    const metadata = "\x00\x00__metadata__:relational_integrity_activation";
    try f.manager.writeIntents(F.id, &.{ .{ .key = metadata, .value = "physical-metadata" }, .{ .key = "removed", .value = null } }, &.{});
    const artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "row", "old");
    defer a.free(artifact);
    try f.store.put(artifact, "existing");
    const frame = try f.frame();
    defer a.free(frame);
    // Even a supplied empty derived request cannot omit the stored user keys.
    try f.manager.sealPreparedEffectsAndReplayPlan(F.id, frame, F.catalog, .deferred, null, .{}, .{}, null);
    {
        var read = try f.store.beginReadTxn();
        defer read.abort();
        const grant = (try F.credit.load(&read, F.id)).?;
        try std.testing.expectEqual(@as(u64, 2), grant.root_count);
        try std.testing.expectEqual(@as(u64, 1), grant.base.changed_doc_keys.count);
        try std.testing.expectEqual(@as(u64, 1), grant.base.deleted_doc_keys.count);
        try std.testing.expectEqual(@as(u64, 1), grant.base.overwritten_doc_keys.count);
        try std.testing.expectEqualDeep(try (@import("artifact_replay_inventory.zig").State{ .count = 1, .key_bytes = artifact.len }).retirementReplayShape(), grant.current);
    }
    var accepted = (try f.manager.loadPreparedEffects(a, F.id, null)).?;
    defer accepted.deinit();
    const payload = try @import("db/derived/change_journal.zig").encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = &.{"row"}, .deleted_doc_keys = &.{ "removed", artifact }, .overwritten_doc_keys = &.{"row"}, .changed_artifact_keys = &.{artifact} });
    defer a.free(payload);
    try std.testing.expect((try f.manager.resolvePreparedIntents(F.id, 2, &accepted, .{ .deletes = &.{artifact}, .replay = .{ .sequence = 1, .payload = payload } }, null)).applied);
    try f.manager.clearReplicationOutbox(F.id, .replay);
}

test "prepared transaction effects assembled auxiliary mutations require preowned roots even when replay fits" {
    const a = std.testing.allocator;
    const F = PreparedReplaySealFixture;
    for ([_]bool{ false, true }) |deleting| for ([_]bool{ false, true }) |covered| {
        var f: F = .{};
        try f.init();
        defer f.deinit();
        const frame = try f.frame();
        defer a.free(frame);
        const request: @import("db/derived/derived_types.zig").DerivedBatch = if (!covered) .{} else if (deleting)
            .{ .deleted_keys = &.{"aux"} }
        else
            .{ .documents = &.{.{ .key = "aux" }} };
        try f.manager.sealPreparedEffectsAndReplayPlan(F.id, frame, F.catalog, .deferred, null, request, .{}, null);
        var accepted = (try f.manager.loadPreparedEffects(a, F.id, null)).?;
        defer accepted.deinit();
        const auxiliary = try internal_keys.documentKeyAlloc(a, "aux");
        defer a.free(auxiliary);
        const changed: []const []const u8 = if (deleting) &.{"row"} else &.{ "row", "aux" };
        const payload = try @import("db/derived/change_journal.zig").encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = changed, .deleted_doc_keys = if (deleting) &.{"aux"} else &.{} });
        defer a.free(payload);
        {
            var read = try f.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(payload.len <= try (try F.credit.load(&read, F.id)).?.budget());
        }
        const writes = [_]docstore.KVPair{.{ .key = auxiliary, .value = "published" }};
        const outcome = f.manager.resolvePreparedIntents(F.id, 2, &accepted, .{
            .writes = if (deleting) &.{} else &writes,
            .deletes = if (deleting) &.{auxiliary} else &.{},
            .replay = .{ .sequence = 1, .payload = payload },
        }, null);
        if (covered) {
            try std.testing.expect((try outcome).applied);
            try std.testing.expect(!try f.manager.hasPreparedEffects(F.id));
            if (!deleting) {
                const value = try f.store.get(a, auxiliary);
                defer a.free(value);
                try std.testing.expectEqualStrings("published", value);
            }
            try f.manager.clearReplicationOutbox(F.id, .replay);
        } else {
            try std.testing.expectError(error.PreparedEffectsMismatch, outcome);
            try std.testing.expect(try f.manager.hasPreparedEffects(F.id));
            try std.testing.expectEqual(TxnStatus.pending, try f.manager.getTransactionStatus(F.id));
            try std.testing.expectError(error.NotFound, f.store.get(a, auxiliary));
            var read = try f.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(F.credit.Phase.prepared, (try F.credit.load(&read, F.id)).?.phase);
        }
    };
}
