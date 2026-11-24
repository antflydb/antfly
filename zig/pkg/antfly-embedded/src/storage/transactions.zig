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
const document_revision_reservation = @import("document_revision_reservation.zig");
pub const topology_debt = @import("transaction_debt.zig");
pub const ExternalDecisionOwner = @import("transaction_decision_owner.zig").External;
pub const NativePhysicalPin = @import("../common/relation_persistence.zig").PhysicalPin;
pub const completion = @import("transaction_completion.zig");
const source_scope = @import("source_scope.zig");
const completion_admission_prefix = completion.admission_prefix;
const wal_trace = @import("write_amplification_trace.zig");

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
// Metadata admission and native read-side cohort work share one limit.
// Intent payload count never determines snapshot admission work.
pub const max_external_prepared_transactions = 64;
const external_read_barrier_count_key = "\x00\x00__txn_external_read_barrier_count";
const external_expiry_floor_key = "\x00\x00__txn_external_expiry_floor";

fn externalReadBarrierCount(view: anytype) !u64 {
    const raw = view.get(external_read_barrier_count_key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    return decodeExternalReadBarrierCount(raw);
}

fn decodeExternalReadBarrierCount(raw: ?[]const u8) !u64 {
    const bytes = raw orelse return 0;
    if (bytes.len != 8) return error.InvalidTxnRecord;
    const count = std.mem.readInt(u64, bytes[0..8], .little);
    if (count == 0) return error.InvalidTxnRecord;
    return count;
}

/// Check once against the exact immutable serving root, including empty scans
/// and absent point reads. This covers insertion/deletion phantoms without
/// looking up a lock for each returned row. Old roots remain readable.
pub fn checkExternalReadAdmission(view: anytype) !void {
    if (try externalReadBarrierCount(view) != 0) return error.IntentConflict;
}

/// Skip each external transaction's entire intent key range, rather than
/// examining its rows. Fail closed at the shared admission limit, including a
/// corrupt/missing owner record. Every observation uses one immutable root.
fn statementReadHasDebt(read: anytype) !bool {
    if (try externalReadBarrierCount(read) != 0) return true;
    var cursor = try read.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(intents_prefix);
    var cohorts: usize = 0;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, intents_prefix)) break;
        if (cohorts == max_external_prepared_transactions) return true;
        if (item.key.len < intents_prefix.len + 17 or item.key[intents_prefix.len + 16] != ':') return error.InvalidTxnRecord;
        const id: TxnId = item.key[intents_prefix.len..][0..16].*;
        const bytes = read.get(&makeRecordKey(id)) catch |err| switch (err) {
            error.NotFound => return error.InvalidTxnRecord,
            else => return err,
        };
        const record = try decodeRecord(bytes);
        if (record.external_decision_owner == null) return true;
        if (record.status != .pending or !record.prepared or record.external_commit_started) return error.InvalidTxnRecord;
        var next: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(next[0..intents_prefix.len], intents_prefix);
        @memcpy(next[intents_prefix.len..][0..16], &id);
        next[intents_prefix.len + 16] = ':' + 1;
        cohorts += 1;
        entry = try cursor.seekAtOrAfter(&next);
    }
    return false;
}

/// Atomic point admission preserves the no-memtable-clone serving fast path.
/// The returned lease owns the borrowed row and must outlive its consumption.
pub fn captureServingValue(store: *DocStore, key: []const u8, value: *?[]const u8) !DocStore.Txn {
    const first = std.mem.order(u8, key, external_read_barrier_count_key) == .lt;
    const keys: [2][]const u8 = if (first) .{ key, external_read_barrier_count_key } else .{ external_read_barrier_count_key, key };
    var values: [2]?[]const u8 = undefined;
    var lease = try store.readManyConsistent(&keys, &values);
    errdefer lease.abort();
    if (try decodeExternalReadBarrierCount(values[if (first) 1 else 0]) != 0) return error.IntentConflict;
    value.* = values[if (first) 0 else 1];
    return lease;
}

pub fn terminalRetentionElapsed(record: anytype, cutoff: u64, retained_cutoff: u64) bool {
    // External fences may be reclaimed only after their admission window.
    // The caller's ordinary/retained cutoff must never advance that deadline.
    if (record.external_decision_owner) |owner| return record.finalized_at != 0 and owner.expires_at_unix_ns < cutoff;
    return record.finalized_at < (if (record.retain_terminal) retained_cutoff else cutoff);
}

pub const max_read_guards_per_transaction = 65_536;
// Durable epoch leases. A prepare vote and its schema identity are one atomic
// mutation; resolution retires both. Historical immutable schemas remain
// usable after a new active epoch is published and after participant restart.
const schema_leases_prefix = "\x00\x00__txn_schema_leases__:";
const records_prefix = completion.record_prefix;
const participants_prefix = "\x00\x00__txn_participants__:";
const participant_index_prefix = "\x00\x00__txn_participant_index_v1__:";
const resolved_index_prefix = "\x00\x00__txn_resolved_index_v1__:";
const resolved_participants_prefix = "\x00\x00__txn_resolved_participants__:";
const hot_standby_batch_outbox_prefix = "\x00\x00__txn_ha_batch_outbox__:";
const hot_standby_replay_outbox_prefix = "\x00\x00__txn_ha_replay_outbox__:";

// ============================================================================
// Types
// ============================================================================

pub const TxnId = [16]u8;

fn optionalTopologyValue(view: anytype, key: []const u8) !?[]const u8 {
    return view.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
fn topologyDebtTxnId(key: []const u8) ?TxnId {
    inline for (.{ records_prefix, participants_prefix, resolved_participants_prefix, participant_index_prefix }) |prefix| {
        if (key.len == prefix.len + 16 and std.mem.startsWith(u8, key, prefix)) return key[prefix.len..][0..16].*;
    }
    return null;
}

const TopologyDebtClassifier = struct {
    pub fn fast(view: anytype, id: TxnId) !?topology_debt.Kind {
        const raw_record = (try optionalTopologyValue(view, &makeRecordKey(id))) orelse return null;
        const record = try decodeRecord(raw_record);
        if (record.status == .pending) return .blocked;
        const raw_index = (try optionalTopologyValue(view, &makeSidecarKey(participant_index_prefix, id))) orelse return .dirty;
        const indexed = try ParticipantIndex.decode(raw_index);
        if (try optionalTopologyValue(view, &makeSidecarKey(resolved_participants_prefix, id)) != null) return .dirty;
        return if (indexed.enlisted != indexed.resolved) .blocked else null;
    }

    /// One list element or one resolution-directory entry per call. Full
    /// cohorts are neither cloned nor reparsed on the serialized apply lane.
    pub fn step(read: anytype, points: anytype, id: TxnId, page: *topology_debt.Page) !topology_debt.Classification {
        if (try optionalTopologyValue(read, &makeRecordKey(id)) == null) return error.InvalidTxnRecord;
        const progress = &page.next.progress;
        if (progress.phase == .none) {
            const kind = try fast(read, id);
            if (kind != .dirty) return .{ .done = kind };
            if (try optionalTopologyValue(read, &makeSidecarKey(participant_index_prefix, id))) |raw| {
                const index = try ParticipantIndex.decode(raw);
                progress.enlisted = index.enlisted;
                progress.indexed_expected = index.resolved;
                progress.expected = index.resolved;
                progress.phase = .indexed;
            } else progress.phase = .enlisted;
            return .more;
        }
        if (progress.phase == .indexed) {
            const prefix = makeSidecarKey(resolved_index_prefix, id);
            var cursor = try read.openCursor();
            defer cursor.close();
            var key: [prefix.len + 32]u8 = undefined;
            @memcpy(key[0..prefix.len], &prefix);
            @memcpy(key[prefix.len..], &progress.after_member);
            const row = if (progress.seen == 0) try cursor.seekAtOrAfter(&prefix) else blk: {
                const found = try cursor.seekAtOrAfter(&key);
                break :blk if (found != null and std.mem.eql(u8, found.?.key, &key)) try cursor.next() else found;
            };
            if (row == null or !std.mem.startsWith(u8, row.?.key, &prefix)) {
                if (progress.seen != progress.indexed_expected) return error.InvalidTxnRecord;
                progress.phase = .indexed_legacy;
                progress.offset = 0;
                progress.seen = 0;
                progress.expected = 0;
                return .more;
            }
            const member = row.?;
            if (member.value.len == 0 or !std.mem.eql(u8, member.key, &makeParticipantIndexKey(resolved_index_prefix, id, member.value))) return error.InvalidTxnRecord;
            const enlisted = (try optionalTopologyValue(points, &makeParticipantIndexKey(participant_index_prefix, id, member.value))) orelse return error.InvalidTxnRecord;
            if (!std.mem.eql(u8, enlisted, member.value)) return error.InvalidTxnRecord;
            progress.after_member = member.key[prefix.len..][0..32].*;
            progress.seen = std.math.add(u64, progress.seen, 1) catch return error.InvalidTxnRecord;
            progress.resolved = progress.seen;
            if (progress.seen > progress.expected or progress.resolved > progress.enlisted) return error.InvalidTxnRecord;
            return .more;
        }
        if (progress.phase != .enlisted and progress.phase != .resolved and progress.phase != .indexed_legacy) return error.InvalidTxnRecord;
        const raw = if (progress.phase == .enlisted) try optionalTopologyValue(read, &makeSidecarKey(participants_prefix, id)) else try optionalTopologyValue(read, &makeSidecarKey(resolved_participants_prefix, id));
        if (raw == null) {
            if (progress.offset != 0) return error.InvalidTxnRecord;
            finishList(progress);
            return .more;
        }
        const bytes = raw.?;
        if (bytes.len < 4) return error.InvalidTxnRecord;
        if (progress.offset == 0) {
            progress.expected = std.mem.readInt(u32, bytes[0..4], .little);
            progress.offset = 4;
        }
        if (progress.offset > bytes.len or progress.seen > progress.expected) return error.InvalidTxnRecord;
        if (progress.seen == progress.expected) {
            if (progress.offset != bytes.len) return error.InvalidTxnRecord;
            finishList(progress);
            return .more;
        }
        const offset: usize = @intCast(progress.offset);
        if (bytes.len - offset < 4) return error.InvalidTxnRecord;
        const length = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        if (length > bytes.len - offset - 4) return error.InvalidTxnRecord;
        const name = bytes[offset + 4 ..][0..length];
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
        if (progress.phase == .enlisted) {
            if (!try page.markSeen(points, 1, digest)) progress.enlisted = std.math.add(u64, progress.enlisted, 1) catch return error.InvalidTxnRecord;
        } else {
            const indexed = progress.phase == .indexed_legacy;
            if (indexed) {
                const enlisted = (try optionalTopologyValue(points, &makeParticipantIndexKey(participant_index_prefix, id, name))) orelse return error.InvalidTxnRecord;
                if (!std.mem.eql(u8, enlisted, name)) return error.InvalidTxnRecord;
            } else if (!try page.wasEnlisted(points, digest)) return error.InvalidTxnRecord;
            if (!try page.markSeen(points, 2, digest)) {
                const existing = if (indexed) try optionalTopologyValue(points, &makeParticipantIndexKey(resolved_index_prefix, id, name)) else null;
                if (existing) |value| {
                    if (!std.mem.eql(u8, value, name)) return error.InvalidTxnRecord;
                } else progress.resolved = std.math.add(u64, progress.resolved, 1) catch return error.InvalidTxnRecord;
            }
            if (progress.resolved > progress.enlisted) return error.InvalidTxnRecord;
        }
        progress.offset += 4 + @as(u64, length);
        progress.seen += 1;
        return .more;
    }
    fn finishList(progress: *topology_debt.Progress) void {
        progress.phase = if (progress.phase == .enlisted) .resolved else .cleanup;
        if (progress.phase == .cleanup) progress.indexed_expected = 0;
        progress.offset = 0;
        progress.seen = 0;
        progress.expected = 0;
        progress.after_member = @splat(0);
    }
};

pub const TxnStatus = @import("source_legacy_inventory.zig").Status;

pub const WriteIntent = struct {
    prepared_graph_effects: ?[]const u8 = null,
    /// Transient internal origin marker, consumed by canonical AROW materialization
    /// before voting prepared. It is never an alternative journal value format.
    prepared_domain: ?*const @import("../schema/domain_handoff.zig").Contract = null,
    row_policy_requirement: @import("../usermgr/row_policy_authority.zig").MutationRequirement = .native,
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

    pub fn json_skip_field(self: @This(), field: []const u8) bool {
        return std.mem.eql(u8, field, "prepared_domain") and self.prepared_domain == null;
    }
};

pub const IntentValue = @import("transaction_intent_value.zig").Value;

/// Admission counts retained input/AROW bytes and per-row commit metadata.
/// Transient parsing and preparation allocations are independently charged by
/// the preparation allocator, rather than multiplying payload bytes by a DOM
/// worst case. Limits apply across prepares; replacing a key releases credits.
pub fn intentAdmissionBytes(intent: WriteIntent) !u64 {
    var payload: u64 = (if (intent.value) |value| value.len else 0) +
        (if (intent.prepared_row) |row| row.len else 0) +
        (if (intent.prepared_graph_effects) |effects| effects.len else 0);
    for (intent.json_null_fields) |field| payload = std.math.add(u64, payload, field.len + @sizeOf([]const u8)) catch return error.TransactionTooLarge;
    const bytes = payload;
    const keys = std.math.mul(u64, intent.key.len, 16) catch return error.TransactionTooLarge;
    return std.math.add(u64, bytes, std.math.add(u64, keys, 4096) catch return error.TransactionTooLarge) catch error.TransactionTooLarge;
}

/// REF3 contains final primary effects/timestamps and integrity records, not
/// derived index/vector writes. AROW metadata replacement preserves length.
/// Document numbers retain their exact source spelling. Only special-field
/// stripping serializes JSON; count its exact output with a bounded writer.
const isRawMetadataIntentKey = @import("transaction_metadata_keys.zig").isRawMetadataKey;

fn isRawMetadataPredicateKey(key: []const u8) bool {
    return isRawMetadataIntentKey(key) or
        std.mem.eql(u8, key, @import("db/relational/indexes/catalog.zig").head_key) or
        (key.len == @import("db/relational/indexes/jobs.zig").progress_prefix.len + range_protection.index_id_bytes and
            std.mem.startsWith(u8, key, @import("db/relational/indexes/jobs.zig").progress_prefix));
}

fn retainedIntentBytes(alloc: Allocator, intent: WriteIntent) !u64 {
    if (@import("db/relational/integrity/contract.zig").isKey(intent.key)) {
        _ = try @import("db/relational/integrity/contract.zig").parseKey(intent.key);
        return std.math.add(u64, 16 + intent.key.len, if (intent.value) |value| value.len else 0) catch error.TransactionTooLarge;
    }
    if (isRawMetadataIntentKey(intent.key)) return 0;
    var payload: u64 = if (intent.prepared_row) |row| row.len else if (intent.value) |value| value.len else 0;
    if (intent.prepared_row == null) if (intent.value) |value| {
        payload = try @import("transaction_intent_value.zig").documentImageBytes(alloc, value);
    };
    const primary = std.math.add(u64, 16 + 2 + internal_keys.encodedComponentLen(intent.key), payload) catch return error.TransactionTooLarge;
    return std.math.add(u64, primary, intent.retained_artifact_bytes) catch error.TransactionTooLarge;
}

fn retainedIntentBytesForLanguage(alloc: Allocator, intent: WriteIntent, sources: bool) !u64 {
    const bytes = try retainedIntentBytes(alloc, intent);
    // A live document may acquire a signed TTL source at resolution. Reserve
    // the full source slot before voting yes; deletes and relational rows do
    // not encode this slot. The retained language cannot change with intents.
    if (sources and bytes != 0 and intent.value != null and intent.prepared_row == null and
        !@import("db/relational/integrity/contract.zig").isKey(intent.key))
        return std.math.add(u64, bytes, 16) catch error.TransactionTooLarge;
    return bytes;
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
    expected_mutation_revision: ?u64 = null,
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
    return bytes.len == 8 or (bytes.len == 33 and (bytes[0] == 2 or bytes[0] == 3 or bytes[0] == 4 or bytes[0] == 5));
}

fn readPredicateIdentity(predicate: VersionPredicate, out: *[33]u8) []const u8 {
    switch (predicate.comparison) {
        .document_version => {
            if (predicate.expected_content_digest) |digest| {
                var version: [8]u8 = undefined;
                std.mem.writeInt(u64, &version, predicate.expected_version, .little);
                var hash = std.crypto.hash.sha2.Sha256.init(.{});
                hash.update(&version);
                hash.update(&digest);
                out[0] = if (predicate.expected_mutation_revision) |revision| tagged: {
                    var bytes: [8]u8 = undefined;
                    std.mem.writeInt(u64, &bytes, revision, .little);
                    hash.update(&bytes);
                    break :tagged 5;
                } else 4;
                hash.final(out[1..33]);
                return out;
            }
            std.mem.writeInt(u64, out[0..8], predicate.expected_version, .little);
            return out[0..8];
        },
        .exact_value => {
            out[0] = if (predicate.expected_value != null) 3 else 2;
            std.crypto.hash.Blake3.hash(predicate.expected_value orelse "", out[1..33], .{});
            return out;
        },
    }
}

/// First-native-root admission must not re-attest a stale image that has
/// already accepted transaction work, even when its rows are still invisible.
pub fn hasAnyTransactionStateInView(read: anytype) !bool {
    return try readHasPrefix(read, records_prefix) or try readHasPrefix(read, intents_prefix) or
        try readHasPrefix(read, intent_locks_prefix) or try readHasPrefix(read, read_guards_prefix) or
        try readHasPrefix(read, read_members_prefix) or try readHasPrefix(read, hot_standby_batch_outbox_prefix) or
        try readHasPrefix(read, hot_standby_replay_outbox_prefix) or
        try readHasPrefix(read, document_revision_reservation.header_prefix) or
        try readHasPrefix(read, document_revision_reservation.rows_prefix);
}

/// Compact native decision state for control journals that share this 2PC
/// owner. Read it through the accepting write transaction, without materializing
/// plans or sampling a different snapshot while transferring resource custody.
pub const ExternalParticipant = struct {
    owner: ?ExternalDecisionOwner,
    native_pin: ?NativePhysicalPin = null,
    status: TxnStatus,
    prepared: bool,
    commit_version: u64,
};
pub fn externalParticipant(txn: anytype, id: TxnId) !?ExternalParticipant {
    const bytes = txn.get(&makeRecordKey(id)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const record = try decodeRecord(bytes);
    return .{ .owner = record.external_decision_owner, .native_pin = record.native_pin, .status = record.status, .prepared = record.prepared, .commit_version = record.commit_version };
}

pub fn requireCollectedExternalParticipant(txn: anytype, id: TxnId, owner: ExternalDecisionOwner, expected: TxnStatus, decision_version: u64) !void {
    if (expected == .pending) return error.ExternalTransactionDecisionChanged;
    if (try externalParticipant(txn, id)) |record| {
        const held = record.owner orelse return error.ExternalTransactionDecisionRequired;
        if (!held.eql(owner) or record.status != expected or (expected == .committed and record.commit_version != decision_version)) return error.ExternalTransactionDecisionChanged;
        try requireResolvedExternalLockControl(txn, id);
    } else {
        const raw = txn.get(external_expiry_floor_key) catch |err| return if (err == error.NotFound) error.ExternalTransactionDecisionChanged else err;
        if (raw.len != 8 or std.mem.readInt(u64, raw[0..8], .little) < owner.expires_at_unix_ns) return error.ExternalTransactionDecisionChanged;
    }
    // A genuinely retired local record is possible after its retry window.
    // Exact native decision permission still owns cleanup; independently prove
    // that no physical intent of that transaction remains in this SAME cut.
    var prefix: [intents_prefix.len + 17]u8 = undefined;
    @memcpy(prefix[0..intents_prefix.len], intents_prefix);
    @memcpy(prefix[intents_prefix.len..][0..16], &id);
    prefix[prefix.len - 1] = ':';
    if (try readHasPrefix(txn, &prefix)) return error.ExternalTransactionDecisionChanged;
}

/// A missing outbox becomes actionable only when the same read cut proves
/// that the exact owner cannot subsequently prepare.
pub fn publicationAbortFence(reader: anytype, id: TxnId, owner: ExternalDecisionOwner) !?@import("../system_catalog/logical_replication_capacity.zig").AbortFence {
    const bytes = reader.get(&makeRecordKey(id)) catch |err| {
        if (err != error.NotFound) return err;
        const floor = try TxnManager.readExternalExpiryFloor(reader);
        return if (floor >= owner.expires_at_unix_ns) .{ .expiry_floor = floor } else null;
    };
    const record = try decodeRecord(bytes);
    if (!(record.external_decision_owner orelse return error.ExternalTransactionDecisionChanged).eql(owner))
        return error.ExternalTransactionDecisionChanged;
    if (record.publication_capture_required) return error.LogicalCaptureReceiptMissing;
    if (record.status != .aborted or !record.intents_resolved_known or !record.intents_resolved) return null;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    return .{ .aborted_record = digest };
}

/// Phase-two lock cleanup under a topology fence must follow actual row
/// intent resolution, read from the SAME accepting writer as that cleanup.
pub fn requireResolvedExternalLockControl(txn: anytype, id: TxnId) !void {
    const bytes = txn.get(&makeRecordKey(id)) catch |err| switch (err) {
        error.NotFound => return error.ExternalTransactionDecisionChanged,
        else => return err,
    };
    const record = try decodeRecord(bytes);
    if (record.external_decision_owner == null or record.status == .pending or !record.intents_resolved_known or !record.intents_resolved) return error.LockOwnerBusy;
}

pub const TxnError = error{
    VersionConflict,
    IntentConflict,
    DecisionConflict,
    TxnNotFound,
    InvalidTxnRecord,
    SourceTransactionIdentityConflict,
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
    ordinary_producer: bool = false,
    legacy_producer: bool = false,
    source_scope: ?source_scope.Reference = null,
    source_purpose: ?[32]u8 = null,
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
    external_decision_owner: ?ExternalDecisionOwner = null,
    native_pin: ?NativePhysicalPin = null,
    external_commit_started: bool = false,
    intents_resolved: bool = false,
    intents_resolved_known: bool = false,
};

/// Owned same-apply snapshot for exact existing native retention custody.
pub const ExternalRetentionSnapshot = struct {
    status: TxnStatus,
    coordinator: bool,
    prepared: bool,
    intents_resolved: bool,
    participants: [][]u8,
    pub fn deinit(self: *ExternalRetentionSnapshot, a: Allocator) void {
        freeParticipantList(a, self.participants);
        self.* = undefined;
    }
};

pub const TxnSummaryPage = struct {
    items: []TxnSummary,
    next_after: ?TxnId,
};

pub const ReplicationOrderedPhase = struct {
    permit: source_apply.contract.Permit,
    physical_root: u128,
    position: @import("db/receipt_position.zig").Position,
};

const SourceRetirement = struct {
    command: @import("source_scope_retirement.zig").Command,
    cut: source_scope.Cut,
    already_fenced: bool,
};

/// Internal recovery of an already durable terminal decision. This can never
/// decide pending work: the native writer rechecks the exact original record.
pub const LegacyTerminalResolution = @import("source_producer_policy.zig").LegacyTerminalResolution;
pub const ResolutionExtraBatch = struct {
    replication_resolution: ?ReplicationOrderedPhase = null,
    publication_ordered_position: ?@FieldType(@import("db/receipt_position.zig").Position, "raft") = null,
    publication_terminal_sink: ?@import("publication_prepare_sink.zig").TerminalSink = null,
    prepared_graph_effects_checked: bool = false,
    external_decision_owner: ?ExternalDecisionOwner = null,
    native_pin: ?NativePhysicalPin = null,
    source_scope_cut: ?source_scope.Cut = null,
    /// Private ordered retirement receipt, staged after scope resolution in the
    /// same writer, including terminal retries without row mutations.
    source_retirement: ?SourceRetirement = null,
    source_scope: ?source_scope.Reference = null,
    completion_owner: ?completion.RoutedOwner = null,
    source_purpose: ?[32]u8 = null,
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
const prepared_enrichments_prefix = "\x00\x00__txn_prepared_enrichments__:";

const source_apply = @import("db/replication_source_apply_store.zig");

pub const ReplicationPrepare = struct {
    preparation: source_apply.Preparation,
    position: @import("db/receipt_position.zig").Position,
    catalog: @import("db/table_catalog.zig").Catalog,
};

pub const ReplicationAcknowledgement = struct {
    completion: source_apply.contract.AckCompletion,
    physical_root: u128,
    position: @import("db/receipt_position.zig").Position,
};

pub const MutationExtraBatch = struct {
    replication_ack: ?ReplicationAcknowledgement = null,
    replication_activation: ?ReplicationOrderedPhase = null,
    replication_prepare: ?ReplicationPrepare = null,
    publication_capture: ?@import("db/logical_replication_outbox.zig").Preparation = null,
    /// Private native owner hook. The sink journals the materialized first
    /// PREPARE in the same backend batch, before the durability ticket exists.
    publication_prepare_sink: ?@import("publication_prepare_sink.zig").Sink = null,
    document_revision_scope: ?document_revision_reservation.Scope = null,
    document_revision_references: []const document_revision_reservation.Reference = &.{},
    external_decision_owner: ?ExternalDecisionOwner = null,
    native_pin: ?NativePhysicalPin = null,
    /// Trusted native ingress only. Never inferred on a replay of an old BEGIN.
    ordinary_producer: bool = false,
    legacy_producer: bool = false,
    source_scope: ?source_scope.Reference = null,
    completion_owner: ?completion.RoutedOwner = null,
    /// Computed by trusted private PREPARE dispatch over the complete original
    /// canonical input. Never supplied directly by public mutation JSON.
    completion_input_digest: ?[32]u8 = null,
    /// Immutable source-command fingerprint installed with BEGIN and required
    /// by PREPARE. Ordinary sessions cannot adopt a source transaction ID.
    source_purpose: ?[32]u8 = null,
    prepared_enrichments_json: ?[]const u8 = null,
    document_revisions: ?bool = null,
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
    /// Logical sources changed by graph-only intents. Reserve their document
    /// ranges just like primary writes, without replacing primary row bytes.
    graph_sources: []const WriteIntent = &.{},
};

pub const SchemaBinding = struct {
    // null explicitly binds a schemaless document table, not the future tip.
    version: ?u32 = null,
};

pub const ResolutionOutcome = struct {
    applied: bool,
    replay_sequence: u64,
};

/// `resolveIntentsWithExtraBatchStaged`'s result: the same `ResolutionOutcome`
/// plus the durability ticket (if any) for whatever this resolution staged.
/// `ticket` is `null` only when resolution wrote nothing (the no-op terminal-
/// retry branch with no completion writes/deletes). See
/// `docs/design/lite-performance/lite-single-commit-write-path.md` §3.2:
/// callers release locks scoped to "a transaction is open" between getting
/// this ticket and calling `wait()` on it, so this resolution's commit can
/// share a group-commit fsync with concurrent writers instead of blocking
/// them behind it.
pub const StagedResolutionOutcome = struct {
    outcome: ResolutionOutcome,
    ticket: ?backend_erased.Ticket = null,
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

pub const ReplicationOutboxKind = enum { batch, replay };

pub const ReplayAppend = struct {
    sequence: u64,
    payload: []const u8,
};

pub const IntentBatch = struct {
    document_revision_references: []document_revision_reservation.Reference = &.{},
    ordinary_producer: bool = false,
    legacy_producer: bool = false,
    prepared_enrichments_json: ?[]u8 = null,
    document_revisions: bool = false,
    writes: []docstore.KVPair = &.{},
    deletes: [][]const u8 = &.{},
    revision: u64 = 0,
    schema_binding: ?SchemaBinding = null,
    prepared_rows: []?[]const u8 = &.{},
    prepared_graph_effects: []?[]const u8 = &.{},
    // Primary slices borrow this owned snapshot. Do not duplicate every
    // payload merely to strip the one-byte intent envelope.
    owned_entries: ?[]backend_scan.OwnedKVPair = null,

    pub fn deinit(self: *IntentBatch, alloc: Allocator) void {
        if (self.document_revision_references.len != 0) alloc.free(self.document_revision_references);
        if (self.prepared_enrichments_json) |bytes| alloc.free(bytes);
        if (self.prepared_rows.len != 0) alloc.free(self.prepared_rows);
        if (self.prepared_graph_effects.len != 0) alloc.free(self.prepared_graph_effects);
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

const TxnRecord = struct {
    publication_capture_required: bool = false,
    replication_source_required: bool = false,
    ordinary_producer: bool = false,
    legacy_producer: bool = false,
    source_scope: ?source_scope.Reference = null,
    source_purpose: ?[32]u8 = null,
    document_revisions: bool = false,
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
    /// A prepared external coordinator retains its vote until the exact
    /// metadata decision owner explicitly resolves it. Age is not a decision.
    external_decision_owner: ?ExternalDecisionOwner = null,
    native_pin: ?NativePhysicalPin = null,
    external_commit_started: bool = false,
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
// Main's exact modern-document shape remains 54 bytes. Unreleased external
// ownership uses a distinct shape with an explicit revision bit, never a
// reused main v7 tag or inference from the current table schema.
const txn_record_v7_size = 54;
const txn_record_external_size = txn_record_v7_size + ExternalDecisionOwner.encoded_len + 1;
const txn_record_external_pinned_size = txn_record_external_size + NativePhysicalPin.encoded_len;
const txn_record_v8_size = 87;
const txn_record_v9_size = txn_record_v8_size + source_scope.Reference.encoded_len;
// Separate exact-size encoding keeps every older record unclassified.
const txn_record_v10_size = txn_record_v7_size + 1;
comptime {
    const published_sizes = [_]usize{ txn_record_v6_size, txn_record_v7_size, txn_record_v8_size, txn_record_v9_size, txn_record_v10_size };
    for (published_sizes) |size| {
        if (txn_record_external_size == size or txn_record_external_pinned_size == size)
            @compileError("external transaction records must have distinct durable sizes");
    }
}

// ============================================================================
// TxnManager
// ============================================================================

/// Apply-fenced semantic reads must not interpret physical bytes protected
/// by another transaction's unresolved exclusive intent. Shared read guards
/// remain compatible. Reuse one lock-key buffer and the caller's point probe;
/// no transaction-record scan or mutable-store snapshot is needed.
/// Strict lock admission must see undecided external write intents too.
/// Ordinary MVCC reads deliberately use the less restrictive IntentReadGuard.
pub fn pendingIntentOwner(a: Allocator, view: anytype, user_key: []const u8, exclude: ?TxnId) !?TxnId {
    const key = try std.mem.concat(a, u8, &.{ intent_locks_prefix, user_key });
    defer a.free(key);
    const bytes = view.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len != @sizeOf(TxnId)) return error.InvalidTxnRecord;
    const owner: TxnId = bytes[0..16].*;
    if (exclude) |id| if (std.mem.eql(u8, &owner, &id)) return null;
    return owner;
}

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
        const record_key = makeRecordKey(owner[0..16].*);
        const raw = view.get(&record_key) catch |err| switch (err) {
            error.NotFound => return error.InvalidTxnRecord,
            else => return err,
        };
        const record = try decodeRecord(raw);
        // Externally prepared writes remain invisible while the external
        // owner is undecided. Commit first activates these existing barriers
        // on EVERY participant, then resolves any row effects. A pinned old
        // snapshot keeps seeing the old record and old row together.
        if (record.status == .pending and record.external_decision_owner != null and !record.external_commit_started) return;
        return TxnError.IntentConflict;
    }
};

pub const TxnManager = struct {
    store: backend_erased.Store,
    owns_store: bool,
    alloc: Allocator,
    native_relation_pin: ?NativePhysicalPin = null,
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
            .native_relation_pin = nativePinForStore(store),
            .trace_writer = if (comptime build_options.with_tla) stderr_writer.stderrAntflyTraceWriter() else null,
        };
    }

    pub fn deinit(self: *TxnManager) void {
        if (self.owns_store) self.store.deinit();
        self.* = undefined;
    }

    pub fn prepareTopologyDebtPage(self: *TxnManager, scope: topology_debt.Scope, control: anytype) !?topology_debt.Page {
        var read = try self.store.beginRead();
        defer read.abort();
        var points = try read.openReadScope(self.alloc);
        defer points.close();
        return try topology_debt.preparePage(&read, &points, scope, records_prefix, TopologyDebtClassifier, control);
    }

    /// Only the schema owner holding the admitted upgraded-writer fence may
    /// call this. The initial page examines at most 64 fixed-size records;
    /// legacy cohort work is explicitly deferred to prepared control pages.
    pub fn stageTopologyDebtBegin(txn: anytype, scope: topology_debt.Scope) !void {
        if (try topology_debt.load(txn)) |existing| if (existing.scope.eql(scope)) {
            if (existing.phase == .retired) return error.SchemaValidationJobChanged;
            return;
        };
        _ = try topology_debt.initialize(txn, scope);
        const Control = struct {
            pub fn ensureActive(_: @This()) !void {}
        };
        if (try topology_debt.preparePage(txn, txn, scope, records_prefix, TopologyDebtClassifier, Control{})) |page| try page.stage(txn);
    }

    pub fn hasPhysicalTopologyDebt(view: anytype) !bool {
        inline for (.{ intents_prefix, intent_locks_prefix, read_guards_prefix, read_members_prefix, schema_leases_prefix, hot_standby_batch_outbox_prefix, hot_standby_replay_outbox_prefix }) |prefix|
            if (try topology_debt.hasPrefix(view, prefix)) return true;
        return false;
    }

    pub fn hasQualifiedTopologyDebt(view: anytype, scope: topology_debt.Scope) !bool {
        return !try topology_debt.drained(view, scope) or try hasPhysicalTopologyDebt(view);
    }

    const SnapshotApi = struct {
        pub const records = records_prefix;
        pub const participants = participants_prefix;
        pub const resolved = resolved_participants_prefix;
        pub const members = participant_index_prefix;
        pub const acks = resolved_index_prefix;
        pub const record = decodeRecord;
        pub const index = ParticipantIndex.decode;
        pub const recordKey = makeRecordKey;
        pub const sidecarKey = makeSidecarKey;
        pub const memberKey = makeParticipantIndexKey;
    };
    pub fn TopologySnapshotAudit(comptime Read: type) type {
        return @import("transaction_snapshot.zig").Audit(Read, SnapshotApi);
    }
    pub fn validateDocumentRevisionSnapshot(alloc: Allocator, read: anytype, control: anytype) !void {
        var points = try read.openReadScope(alloc);
        defer points.close();
        var cursor = try read.openCursor();
        defer cursor.close();
        var row = try cursor.seekAtOrAfter(document_revision_reservation.header_prefix);
        while (row) |entry| {
            try control.check();
            if (!std.mem.startsWith(u8, entry.key, document_revision_reservation.header_prefix)) break;
            if (entry.key.len != document_revision_reservation.header_prefix.len + 16) return error.InvalidTxnRecord;
            const id = entry.key[document_revision_reservation.header_prefix.len..][0..16].*;
            const header = try document_revision_reservation.Header.decode(entry.value);
            if (!std.mem.eql(u8, &header.scope.transaction_id, &id)) return error.InvalidTxnRecord;
            try points.reset();
            const record = try decodeRecord(try points.get(&makeRecordKey(id)));
            if (!record.document_revisions or record.begin_timestamp != header.scope.begin_timestamp) return error.InvalidTxnRecord;
            if (header.owner) |owner| {
                if (!(record.external_decision_owner orelse return error.InvalidTxnRecord).eql(owner)) return error.InvalidTxnRecord;
            } else if (record.external_decision_owner != null or record.prepared or record.intent_revision != 0 or record.status == .committed) return error.InvalidTxnRecord;
            if (header.scope.native_pin) |pin| {
                if (record.external_decision_owner != null and !(record.native_pin orelse return error.InvalidTxnRecord).eql(pin)) return error.InvalidTxnRecord;
            } else if (record.native_pin != null) return error.InvalidTxnRecord;
            const watermark_bytes = try points.get(ttl.document_version_watermark_key);
            if (watermark_bytes.len != 8) return error.InvalidTimestamp;
            const watermark = std.mem.readInt(u64, watermark_bytes[0..8], .little);
            const catalog = try @import("db/table_catalog.zig").Catalog.decode(try points.get(@import("db/table_catalog.zig").key));
            if (catalog.transaction_effect_format < 2) return error.InvalidTxnRecord;
            var children = try read.openCursor();
            defer children.close();
            const prefix = document_revision_reservation.rowPrefix(id);
            var child = try children.seekAtOrAfter(&prefix);
            var count: usize = 0;
            var key_bytes: usize = 0;
            while (child) |item| {
                try control.check();
                if (!std.mem.startsWith(u8, item.key, &prefix)) break;
                count += 1;
                if (item.key.len == prefix.len or count > header.count) return error.InvalidTxnRecord;
                key_bytes = std.math.add(usize, key_bytes, item.key.len - prefix.len) catch return error.InvalidTxnRecord;
                if (key_bytes > header.key_bytes) return error.InvalidTxnRecord;
                const version = try document_revision_reservation.rowVersion(item.value);
                if (version > watermark) return error.InvalidTimestamp;
                if (!record.intents_resolved_known or !record.intents_resolved) {
                    try points.reset();
                    const lock_key = try std.mem.concat(alloc, u8, &.{ intent_locks_prefix, item.key[prefix.len..] });
                    defer alloc.free(lock_key);
                    const held = try points.get(lock_key);
                    if (held.len != 16 or !std.mem.eql(u8, held, &id)) return error.InvalidTxnRecord;
                }
                if (item.value.len >= 16 and catalog.transaction_effect_format < 3) return error.InvalidTxnRecord;
                if (try document_revision_reservation.rowUsed(item.value)) {
                    if (!record.prepared or record.intent_revision == 0) return error.InvalidTxnRecord;
                    if (!record.intents_resolved_known or !record.intents_resolved) {
                        try points.reset();
                        const key = try std.mem.concat(alloc, u8, &.{ &makeSidecarKey(intents_prefix, id), ":", item.key[prefix.len..] });
                        defer alloc.free(key);
                        if ((try IntentValue.decode(try points.get(key))).document_revision != version) return error.InvalidTxnRecord;
                    }
                }
                child = try children.next();
            }
            if (count != header.count or key_bytes != header.key_bytes) return error.InvalidTxnRecord;
            row = try cursor.next();
        }
        // The reverse proof catches orphan rows even when every header above
        // was valid. This pass retains one key and one fixed-size header only.
        row = try cursor.seekAtOrAfter(document_revision_reservation.rows_prefix);
        while (row) |entry| {
            try control.check();
            if (!std.mem.startsWith(u8, entry.key, document_revision_reservation.rows_prefix)) break;
            if (entry.key.len <= document_revision_reservation.rows_prefix.len + 17 or entry.key[document_revision_reservation.rows_prefix.len + 16] != ':') return error.InvalidTxnRecord;
            const id = entry.key[document_revision_reservation.rows_prefix.len..][0..16].*;
            try points.reset();
            _ = try document_revision_reservation.Header.decode(try points.get(&document_revision_reservation.headerKey(id)));
            row = try cursor.next();
        }
    }

    pub fn validateTopologyDebtSnapshot(alloc: Allocator, read: anytype, expected: ?topology_debt.Scope, options: @import("../spill_sort.zig").Options) !topology_debt.SnapshotStats {
        var points = try read.openReadScope(alloc);
        defer points.close();
        const control = options.cancellation orelse @import("antfly_cancellation").CancellationToken.none;
        const stats = try topology_debt.validateSnapshot(read, &points, expected, control);
        try @import("transaction_snapshot.zig").validate(alloc, read, try topology_debt.load(&points), options, SnapshotApi);
        try validateDocumentRevisionSnapshot(alloc, read, control);
        return stats;
    }

    /// Create a new pending transaction record.
    pub fn initTransaction(self: *TxnManager, txn_id: TxnId, timestamp: u64) !void {
        try self.initTransactionWithParticipants(txn_id, timestamp, &.{});
    }

    pub fn initTransactionWithParticipants(self: *TxnManager, txn_id: TxnId, timestamp: u64, participants: []const []const u8) !void {
        try self.initTransactionWithParticipantsCreatedAt(txn_id, timestamp, timestamp, participants);
    }

    /// Trusted coordinator admission. The original physical plan and BEGIN
    /// commit in one native batch, including the caller's Raft replay marker.
    /// This does not enable outbound completion until retention credits,
    /// participant resolution and certificate propagation are qualified.
    pub fn initTransactionWithCompletionAdmission(self: *TxnManager, admission: completion.Admission, timestamp: u64, created_at: u64, retain_terminal: bool, extra: MutationExtraBatch) !void {
        return self.initCompletionAdmission(admission, timestamp, created_at, retain_terminal, true, extra);
    }

    pub fn initParticipantWithCompletionAdmission(self: *TxnManager, admission: completion.Admission, timestamp: u64, created_at: u64, retain_terminal: bool, extra: MutationExtraBatch) !void {
        {
            var probe = try self.store.beginProbe();
            defer probe.abort();
            _ = (try @import("committed_history.zig").load(&probe)) orelse return error.HistoryResynchronizationRequired;
        }
        return self.initCompletionAdmission(admission, timestamp, created_at, retain_terminal, false, extra);
    }

    fn initCompletionAdmission(self: *TxnManager, admission: completion.Admission, timestamp: u64, created_at: u64, retain_terminal: bool, coordinator: bool, extra: MutationExtraBatch) !void {
        try admission.validate();
        if (admission.version < 3 and try self.keyExists(@import("committed_history.zig").policy_key)) return error.ChangefeedCapabilityUnsupported;
        const namespace = try self.getAlloc(self.alloc, &internal_keys.identity_namespace_key);
        defer self.alloc.free(namespace);
        if (namespace.len != 24) return TxnError.DecisionConflict;
        const local = if (admission.version == completion.physical_participant_version) local: {
            const owner = extra.completion_owner orelse return error.InvalidHistoryAdmission;
            if (!std.mem.eql(u8, namespace, &owner.namespace)) return error.InvalidParticipant;
            break :local try admission.memberForOwner(owner);
        } else try admission.member(namespace[0..24].*);
        if (coordinator != admission.isCoordinator(local)) return TxnError.DecisionConflict;
        const key = makeSidecarKey(completion_admission_prefix, admission.transaction_id);
        for (extra.writes) |write| if (std.mem.eql(u8, write.key, &key)) return error.InvalidTxnRecord;
        for (extra.deletes) |deleted| if (std.mem.eql(u8, deleted, &key)) return error.InvalidTxnRecord;
        const existing_record = self.loadTransactionRecord(admission.transaction_id) catch |err| switch (err) {
            TxnError.TxnNotFound => null,
            else => return err,
        };
        if (existing_record == null and try self.keyExists(&completion.resolutionKey(admission.transaction_id))) return TxnError.DecisionConflict;
        if (try self.getCompletionAdmission(self.alloc, admission.transaction_id)) |saved| {
            var owned = saved;
            defer owned.deinit();
            if (existing_record == null or !std.mem.eql(u8, &owned.value.digest, &admission.digest())) return TxnError.DecisionConflict;
            const saved_local = try owned.value.localMember(namespace[0..24].*);
            if (!std.mem.eql(u8, &saved_local.owner_fence, &local.owner_fence)) return TxnError.DecisionConflict;
        } else if (existing_record != null) return TxnError.DecisionConflict;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var members: std.ArrayList([]const u8) = .empty;
        var unique: std.StringHashMapUnmanaged(void) = .empty;
        const owner = try admission.coordinatorMember();
        try members.append(scratch, owner.locator);
        for (admission.participants) |participant| {
            const member = try unique.getOrPut(scratch, participant.locator);
            if (member.found_existing) return error.InvalidParticipant;
            if (coordinator and !admission.isCoordinator(participant)) try members.append(scratch, participant.locator);
        }
        // Preserve the original encoding as well as its logical identity. A
        // serializer upgrade must not reprice credits or break pending retry.
        const encoded = if (existing_record != null)
            try self.getAlloc(scratch, &key)
        else if (admission.version == completion.physical_participant_version)
            try completion.encodeLocalAdmission(scratch, admission, local.routed_owner.?)
        else
            try completion.encodeAdmission(scratch, admission);
        var writes: std.ArrayList(docstore.KVPair) = .empty;
        try writes.appendSlice(scratch, extra.writes);
        try writes.append(scratch, .{ .key = &key, .value = encoded });
        var with_admission = extra;
        with_admission.writes = writes.items;
        try self.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(admission.transaction_id, timestamp, created_at, members.items, coordinator, retain_terminal, with_admission);
    }

    pub fn getCompletionAdmission(self: *TxnManager, a: Allocator, txn_id: TxnId) !?std.json.Parsed(completion.AdmissionEnvelope) {
        const key = makeSidecarKey(completion_admission_prefix, txn_id);
        const bytes = self.getAlloc(a, &key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer a.free(bytes);
        var parsed = try completion.parseAdmission(a, bytes);
        errdefer parsed.deinit();
        if (!std.mem.eql(u8, &parsed.value.admission.transaction_id, &txn_id)) return error.InvalidTxnRecord;
        const record = try self.loadTransactionRecord(txn_id);
        if (!record.coordinator_known) return error.InvalidTxnRecord;
        const namespace = try self.getAlloc(a, &internal_keys.identity_namespace_key);
        defer a.free(namespace);
        if (namespace.len != 24) return TxnError.DecisionConflict;
        const participant = try parsed.value.localMember(namespace[0..24].*);
        if (record.coordinator != parsed.value.admission.isCoordinator(participant)) return TxnError.DecisionConflict;
        return parsed;
    }

    pub fn getCompletionResolution(self: *TxnManager, a: Allocator, txn_id: TxnId) !?std.json.Parsed(completion.ResolutionEnvelope) {
        var plan = (try self.getCompletionAdmission(a, txn_id)) orelse return null;
        defer plan.deinit();
        const record = try self.loadTransactionRecord(txn_id);
        if (record.status != .committed or !record.prepared_known or !record.prepared or !record.intents_resolved_known or !record.intents_resolved) return error.SourceOutcomeUnresolved;
        const manifest = try plan.value.admission.manifest(record.commit_version);
        const raw = self.getAlloc(a, &completion.resolutionKey(txn_id)) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer a.free(raw);
        var parsed = try completion.parseResolution(a, manifest, raw);
        errdefer parsed.deinit();
        const namespace = try self.getAlloc(a, &internal_keys.identity_namespace_key);
        defer a.free(namespace);
        if (!std.mem.eql(u8, namespace, &parsed.value.resolution.namespace)) return error.InvalidTxnRecord;
        return parsed;
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

    pub fn initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        timestamp: u64,
        created_at: u64,
        participants: []const []const u8,
        coordinator: bool,
        retain_terminal: bool,
        extra_batch: MutationExtraBatch,
    ) !void {
        // Attribution must wrap the wait too: the group-commit member this
        // stages is only actually applied (and so only fsynced) once
        // something drives/waits its group, inside `ticket.wait()` below,
        // not at `commitStaged`/`enqueue` time -- pushing the tag only
        // around the staged call would have it popped before the real I/O
        // runs, misattributing this commit to `.other`.
        const wal_trace_scope = wal_trace.scopeDefault(.relational_txn_begin);
        defer wal_trace_scope.end();
        const ticket = try self.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatchStaged(
            txn_id,
            timestamp,
            created_at,
            participants,
            coordinator,
            retain_terminal,
            extra_batch,
        );
        try ticket.wait();
    }

    /// Staged counterpart of `initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch`:
    /// stages this transaction's begin record and returns a durability
    /// ticket instead of blocking for the fsync, mirroring
    /// `resolveIntentsWithExtraBatchStaged`'s split (lite-single-commit-
    /// write-path.md §3.2/§3.2a). A caller that stages this across more
    /// than one physical commit domain, or that needs to read this
    /// transaction's record back before this ticket is durable, must not
    /// defer `wait()`: a group-commit member is only applied -- and so only
    /// readable -- once something drives/waits its group (see
    /// `writeTransactionInternal`'s doc comment in `storage/db/db.zig`).
    pub fn initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatchStaged(
        self: *TxnManager,
        txn_id: TxnId,
        timestamp: u64,
        created_at: u64,
        participants: []const []const u8,
        coordinator: bool,
        retain_terminal: bool,
        extra_batch: MutationExtraBatch,
    ) !backend_erased.Ticket {
        try self.requireNativeMutationPin(extra_batch.native_pin);
        if (extra_batch.external_decision_owner) |owner| {
            try owner.validate();
            if (!retain_terminal) return error.ExternalTransactionDecisionRequired;
            if (owner.expires_at_unix_ns <= try self.externalExpiryFloor()) return error.ExternalTransactionAdmissionExpired;
        }
        if (try self.keyExists(@import("committed_history.zig").policy_key)) {
            const completion_key = makeSidecarKey(completion_admission_prefix, txn_id);
            var qualified = try self.keyExists(&completion_key);
            for (extra_batch.writes) |write| if (std.mem.eql(u8, write.key, &completion_key)) {
                qualified = true;
            };
            if (!qualified) return error.ChangefeedCapabilityUnsupported;
        }
        if ((extra_batch.ordinary_producer and extra_batch.legacy_producer) or ((extra_batch.ordinary_producer or extra_batch.legacy_producer) and (extra_batch.source_purpose != null or extra_batch.source_scope != null))) return error.SourceTransactionIdentityConflict;
        if (extra_batch.source_purpose) |purpose| if (std.mem.allEqual(u8, &purpose, 0)) return error.SourceTransactionIdentityConflict;
        if (extra_batch.source_scope) |scope| {
            try scope.validate();
            if (extra_batch.source_purpose == null or !retain_terminal) return error.SourceTransactionIdentityConflict;
        }
        const key = makeRecordKey(txn_id);
        const existing = self.loadTransactionRecord(txn_id) catch |err| switch (err) {
            TxnError.TxnNotFound => null,
            else => return err,
        };
        if (extra_batch.document_revision_scope != null and existing == null) return error.InvalidTxnRecord;
        existing_check: {
            if (existing) |record| {
                const completion_key = makeSidecarKey(completion_admission_prefix, txn_id);
                const frozen = self.getAlloc(self.alloc, &completion_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                defer if (frozen) |bytes| self.alloc.free(bytes);
                if (frozen) |bytes| {
                    var supplied = false;
                    for (extra_batch.writes) |write| if (std.mem.eql(u8, write.key, &completion_key)) {
                        if (supplied or !std.mem.eql(u8, write.value, bytes)) return TxnError.DecisionConflict;
                        supplied = true;
                    };
                    if (!supplied) return TxnError.DecisionConflict;
                    for (extra_batch.deletes) |deleted| if (std.mem.eql(u8, deleted, &completion_key)) return TxnError.DecisionConflict;
                }
                if (record.ordinary_producer != extra_batch.ordinary_producer or record.legacy_producer != extra_batch.legacy_producer or !samePurpose(record.source_purpose, extra_batch.source_purpose) or !sameSourceScope(record.source_scope, extra_batch.source_scope)) return error.SourceTransactionIdentityConflict;
                if (record.external_decision_owner == null and extra_batch.external_decision_owner != null and extra_batch.document_revision_scope != null) {
                    const scope = extra_batch.document_revision_scope.?;
                    try self.requireDocumentRevisionScope(txn_id, scope);
                    var read = try self.store.beginProbe();
                    defer read.abort();
                    const reservation = (try document_revision_reservation.load(&read, txn_id)) orelse return error.ExternalTransactionDecisionChanged;
                    const owner = extra_batch.external_decision_owner.?;
                    try owner.requireFreshVoteAt(created_at);
                    if (!std.mem.eql(u8, &owner.metadata_incarnation, &scope.metadata_incarnation)) return error.ExternalTransactionDecisionChanged;
                    if (!reservation.scope.eql(scope) or scope.begin_timestamp != timestamp or reservation.owner != null or reservation.expires_at <= created_at or
                        record.status != .pending or record.prepared or record.intent_revision != 0 or record.begin_timestamp != timestamp or
                        !record.document_revisions or record.coordinator or record.retain_terminal) return error.ExternalTransactionDecisionChanged;
                    const previous = try self.getParticipants(self.alloc, txn_id);
                    defer freeParticipantList(self.alloc, previous);
                    if (previous.len != 0) return error.ExternalTransactionDecisionChanged;
                    break :existing_check;
                }
                if (extra_batch.document_revision_scope == null and record.external_decision_owner != null) {
                    var read = try self.store.beginProbe();
                    defer read.abort();
                    if (try document_revision_reservation.load(&read, txn_id) != null) return error.ExternalTransactionDecisionRequired;
                }
                if (record.external_decision_owner) |held| {
                    const requested = extra_batch.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
                    if (!held.eql(requested)) return error.ExternalTransactionDecisionChanged;
                } else if (extra_batch.external_decision_owner != null) return error.ExternalTransactionDecisionChanged;
                if (extra_batch.document_revision_scope) |scope| {
                    try self.requireDocumentRevisionScope(txn_id, scope);
                    var read = try self.store.beginProbe();
                    defer read.abort();
                    const header = (try document_revision_reservation.load(&read, txn_id)) orelse return error.ExternalTransactionDecisionChanged;
                    if (!header.scope.eql(scope) or !(header.owner orelse return error.ExternalTransactionDecisionRequired).eql(extra_batch.external_decision_owner orelse return error.ExternalTransactionDecisionRequired)) return error.ExternalTransactionDecisionChanged;
                }
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
                    const upgraded_value = try self.encodeRecord(upgraded);
                    defer self.alloc.free(upgraded_value);
                    var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
                    defer writes.deinit(self.alloc);
                    try writes.append(self.alloc, .{ .key = &key, .value = upgraded_value });
                    try writes.appendSlice(self.alloc, extra_batch.writes);
                    return try self.applyBatchStaged(writes.items, extra_batch.deletes, null, null, null);
                }
                return try self.applyMutationExtraBatchStaged(extra_batch);
            }
        }
        var reservation_header: ?[document_revision_reservation.Header.encoded_len]u8 = null;
        if (extra_batch.document_revision_scope) |scope| {
            try self.requireDocumentRevisionScope(txn_id, scope);
            var read = try self.store.beginProbe();
            defer read.abort();
            var held = (try document_revision_reservation.load(&read, txn_id)) orelse return error.ExternalTransactionDecisionChanged;
            if (!held.scope.eql(scope) or held.owner != null) return error.ExternalTransactionDecisionChanged;
            held.owner = extra_batch.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
            reservation_header = try held.encode();
        }
        const record = TxnRecord{
            .document_revisions = reservation_header != null,
            .external_decision_owner = extra_batch.external_decision_owner,
            .native_pin = if (extra_batch.external_decision_owner != null) extra_batch.native_pin orelse self.native_relation_pin else null,
            .ordinary_producer = extra_batch.ordinary_producer,
            .legacy_producer = extra_batch.legacy_producer,
            .source_purpose = extra_batch.source_purpose,
            .source_scope = extra_batch.source_scope,
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
        var writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
        defer writes.deinit(self.alloc);
        try writes.append(self.alloc, .{ .key = &key, .value = record_value });
        const revision_header_key = document_revision_reservation.headerKey(txn_id);
        if (reservation_header) |*bytes| try writes.append(self.alloc, .{ .key = &revision_header_key, .value = bytes });
        if (participant_value) |value| {
            try writes.append(self.alloc, .{ .key = &participant_key, .value = value });
        }
        // New transactions start with an immutable membership index. Later
        // ACKs and local resolution can update one bounded counter/member
        // pair, rather than rewriting and decoding a growing cohort list.
        const member_keys = try self.alloc.alloc([participant_index_prefix.len + 48]u8, participants.len);
        defer self.alloc.free(member_keys);
        var unique: std.StringHashMapUnmanaged(void) = .empty;
        defer unique.deinit(self.alloc);
        for (participants, 0..) |participant, i| {
            if (participant.len == 0) return error.InvalidParticipant;
            const slot = try unique.getOrPut(self.alloc, participant);
            if (slot.found_existing) continue;
            member_keys[i] = makeParticipantIndexKey(participant_index_prefix, txn_id, participant);
            try writes.append(self.alloc, .{ .key = &member_keys[i], .value = participant });
        }
        const index_key = makeSidecarKey(participant_index_prefix, txn_id);
        var index_value: [17]u8 = undefined;
        (ParticipantIndex{ .enlisted = unique.count() }).encode(&index_value);
        try writes.append(self.alloc, .{ .key = &index_key, .value = &index_value });
        try writes.appendSlice(self.alloc, extra_batch.writes);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);
        if (participants.len > 0) {
            try deletes.append(self.alloc, &resolved_key);
        } else {
            try deletes.appendSlice(self.alloc, &.{ &participant_key, &resolved_key });
        }
        try deletes.appendSlice(self.alloc, extra_batch.deletes);
        const ticket = try self.applyBatchStaged(writes.items, deletes.items, null, null, null);
        if (self.trace_writer) |tw| {
            tw.traceEvent(&.{
                .name = "InitTransaction",
                .txn_id = txn_id,
                .shard_id = self.shard_id,
                .timestamp = timestamp,
            });
        }
        return ticket;
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

    /// A true result means the original prepared images are already durable.
    /// Check before materializing transforms/defaults on a retry. Null denotes
    /// an ordinary or legacy transaction, never a v2 prepare authorization.
    pub fn completionPrepareStatus(self: *TxnManager, txn_id: TxnId, input_digest: ?[32]u8, source_purpose: ?[32]u8, owner: ?completion.RoutedOwner) !?bool {
        return self.completionPrepareScopedStatus(txn_id, input_digest, source_purpose, owner, null);
    }

    pub fn completionPrepareScopedStatus(self: *TxnManager, txn_id: TxnId, input_digest: ?[32]u8, source_purpose: ?[32]u8, owner: ?completion.RoutedOwner, scope: ?source_scope.Reference) !?bool {
        return self.completionPrepareProducerStatus(txn_id, input_digest, source_purpose, owner, scope, false, false);
    }

    pub fn completionPrepareProducerStatus(self: *TxnManager, txn_id: TxnId, input_digest: ?[32]u8, source_purpose: ?[32]u8, owner: ?completion.RoutedOwner, scope: ?source_scope.Reference, ordinary_producer: bool, legacy_producer: bool) !?bool {
        var frozen = (try self.getCompletionAdmission(self.alloc, txn_id)) orelse {
            if (owner != null) return error.SourceTransactionIdentityConflict;
            if (ordinary_producer or legacy_producer) {
                const record = try self.loadTransactionRecord(txn_id);
                if (record.ordinary_producer != ordinary_producer or record.legacy_producer != legacy_producer) return error.SourceTransactionIdentityConflict;
            }
            return null;
        };
        defer frozen.deinit();
        const plan = frozen.value.admission;
        if (plan.version < 3 and try self.keyExists(@import("committed_history.zig").policy_key)) return error.ChangefeedCapabilityUnsupported;
        if (plan.version < 2) {
            if (input_digest != null or owner != null) return error.ChangefeedCapabilityUnsupported;
            return null;
        }
        const namespace = try self.getAlloc(self.alloc, &internal_keys.identity_namespace_key);
        defer self.alloc.free(namespace);
        if (namespace.len != 24) return error.InvalidTxnRecord;
        const member = try frozen.value.localMember(namespace[0..24].*);
        const supplied = input_digest orelse return error.SourceTransactionIdentityConflict;
        if (!std.mem.eql(u8, &supplied, &member.input_digest.?)) return error.SourceTransactionIdentityConflict;
        const binding = owner orelse return error.InvalidHistoryAdmission;
        if (!std.mem.eql(u8, &binding.namespace, namespace)) return error.HistoryResynchronizationRequired;
        try plan.requireRoutedOwner(binding);
        if (!std.mem.eql(u8, &member.owner_fence, &binding.digest())) return error.InvalidParticipant;
        var probe = try self.store.beginProbe();
        defer probe.abort();
        try @import("committed_history.zig").requireCompletionReservation(&probe, namespace[0..24].*, plan);
        const record = try self.loadTransactionRecord(txn_id);
        if (record.ordinary_producer != ordinary_producer or record.legacy_producer != legacy_producer or !samePurpose(record.source_purpose, source_purpose) or !sameSourceScope(record.source_scope, scope)) return error.SourceTransactionIdentityConflict;
        if (record.status != .pending) return TxnError.DecisionConflict;
        return record.prepared_known and record.prepared;
    }

    pub fn writeIntentsExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        intents: []const WriteIntent,
        predicates: []const VersionPredicate,
        extra_batch: MutationExtraBatch,
    ) !void {
        // See `initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch`'s
        // comment: the tag must wrap `wait()` too, since the actual apply/
        // fsync happens there, not at `commitStaged`/`enqueue` time.
        const wal_trace_scope = wal_trace.scopeDefault(.relational_intent_write);
        defer wal_trace_scope.end();
        const ticket = try self.writeIntentsExtraBatchStaged(txn_id, intents, predicates, extra_batch);
        try ticket.wait();
    }

    /// Staged counterpart of `writeIntentsExtraBatch`: stages this write's
    /// intents (and their admission/reservation bookkeeping) and returns a
    /// durability ticket instead of blocking for the fsync, mirroring
    /// `resolveIntentsWithExtraBatchStaged`'s split (lite-single-commit-
    /// write-path.md §3.2/§3.2a). This itself reads back the transaction
    /// record an earlier `initTransaction*` call wrote, so a caller must
    /// not defer that earlier call's `wait()` past this one: a group-commit
    /// member is only applied -- and so only readable -- once something
    /// drives/waits its group, not at `enqueue` time (see
    /// `writeTransactionInternal`'s doc comment in `storage/db/db.zig`).
    pub fn writeIntentsExtraBatchStaged(
        self: *TxnManager,
        txn_id: TxnId,
        intents: []const WriteIntent,
        predicates: []const VersionPredicate,
        extra_batch: MutationExtraBatch,
    ) !backend_erased.Ticket {
        try self.requireNativeMutationPin(extra_batch.native_pin);
        if (extra_batch.document_revision_scope == null and extra_batch.document_revision_references.len != 0) return error.InvalidTxnRecord;
        if (extra_batch.index_span_digests.len > max_read_guards_per_transaction) return error.TransactionTooLarge;
        for (intents) |intent| {
            if (intent.prepared_graph_effects) |effects| {
                var parsed_effects = try @import("db/prepared_graph_effects.zig").decode(extra_batch.preparation_allocator orelse self.alloc, effects, intent.key);
                defer parsed_effects.deinit();
            }
            if (intent.prepared_domain != null or (intent.json_null_fields.len != 0 and intent.prepared_row == null)) return error.PreparedIntentRequiresMaterialization;
        }
        var record = try self.loadTransactionRecord(txn_id);
        if (record.status != .pending) return TxnError.DecisionConflict;
        if (record.external_decision_owner) |owner| {
            const requested = extra_batch.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
            if (!owner.eql(requested)) return error.ExternalTransactionDecisionChanged;
        } else if (extra_batch.external_decision_owner != null) return error.ExternalTransactionDecisionChanged;
        if (record.external_decision_owner) |owner| {
            if (owner.expires_at_unix_ns <= try self.externalExpiryFloor()) return error.ExternalTransactionAdmissionExpired;
        }
        if (record.ordinary_producer != extra_batch.ordinary_producer or record.legacy_producer != extra_batch.legacy_producer or !samePurpose(record.source_purpose, extra_batch.source_purpose) or !sameSourceScope(record.source_scope, extra_batch.source_scope)) return error.SourceTransactionIdentityConflict;
        if (try self.getCompletionAdmission(self.alloc, txn_id)) |saved| {
            var frozen = saved;
            defer frozen.deinit();
            const namespace = try self.getAlloc(self.alloc, &internal_keys.identity_namespace_key);
            defer self.alloc.free(namespace);
            if (namespace.len != 24) return error.InvalidTxnRecord;
            if (frozen.value.admission.version < 3 and try self.keyExists(@import("committed_history.zig").policy_key)) return error.ChangefeedCapabilityUnsupported;
            const participant = try frozen.value.localMember(namespace[0..24].*);
            if (frozen.value.admission.version >= 2) {
                const supplied = extra_batch.completion_input_digest orelse return error.SourceTransactionIdentityConflict;
                if (!std.mem.eql(u8, &supplied, &participant.input_digest.?)) return error.SourceTransactionIdentityConflict;
                const binding = extra_batch.completion_owner orelse return error.InvalidHistoryAdmission;
                if (!std.mem.eql(u8, &binding.namespace, namespace)) return error.HistoryResynchronizationRequired;
                try frozen.value.admission.requireRoutedOwner(binding);
                if (!std.mem.eql(u8, &participant.owner_fence, &binding.digest())) return error.InvalidParticipant;
            } else if (extra_batch.completion_input_digest != null or extra_batch.completion_owner != null) return error.ChangefeedCapabilityUnsupported;
            for (intents) |intent| if (@import("source_checkpoint.zig").isKey(intent.key) and (record.source_purpose == null or participant.roles & completion.Roles.checkpoint == 0)) return error.InvalidParticipant;
            if (participant.roles & completion.Roles.effects == 0) {
                if (extra_batch.graph_sources.len != 0) return error.InvalidParticipant;
                for (intents) |intent| if (participant.roles & completion.Roles.checkpoint == 0 or !isRawMetadataIntentKey(intent.key)) return error.InvalidParticipant;
            }
            var probe = try self.store.beginProbe();
            defer probe.abort();
            try @import("committed_history.zig").requireCompletionReservation(&probe, namespace[0..24].*, frozen.value.admission);
        } else if (extra_batch.completion_owner != null) return error.SourceTransactionIdentityConflict else if (try self.keyExists(@import("committed_history.zig").policy_key)) return error.ChangefeedCapabilityUnsupported;
        const first_prepare = record.prepared_known and !record.prepared and record.intent_revision == 0;
        if (record.publication_capture_required) return error.PreparedIntentRequiresMaterialization;
        if (record.replication_source_required) return error.PreparedIntentRequiresMaterialization;
        if (extra_batch.replication_prepare) |source| {
            try source.preparation.validate();
            try source.position.validate();
            const permit = source.preparation.permit;
            const owner = record.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
            if (!first_prepare or record.native_pin != null or source.position != .raft or extra_batch.publication_prepare_sink != null or
                !owner.eql(permit.decision_owner) or !std.mem.eql(u8, &txn_id, &permit.reference.txn_id)) return error.ReplicationCutoverAuthorityLost;
        }
        const capturing = extra_batch.publication_capture != null or extra_batch.publication_prepare_sink != null;
        if (capturing and (!first_prepare or record.external_decision_owner == null or record.native_pin != null)) return error.InvalidCommitMetadata;
        if (extra_batch.publication_capture) |capture| {
            if (!record.external_decision_owner.?.eql(capture.demand.owner)) return error.ExternalTransactionDecisionChanged;
            if ((capture.ordered == null) != (extra_batch.publication_prepare_sink != null)) return error.InvalidCommitMetadata;
        } else if (extra_batch.publication_prepare_sink != null and !@import("builtin").is_test) return error.InvalidCommitMetadata;
        if (!first_prepare and extra_batch.prepared_enrichments_json == null) {
            if (try self.loadPreparedEnrichments(self.alloc, txn_id)) |packet| {
                self.alloc.free(packet);
                return error.VersionConflict;
            }
        }

        const schema_lease_key = makeSidecarKey(schema_leases_prefix, txn_id);
        var schema_lease_value: [5]u8 = @splat(0);
        const binding = extra_batch.schema_binding orelse SchemaBinding{};
        schema_lease_value[0] = @intFromBool(binding.version != null);
        if (binding.version) |version|
            std.mem.writeInt(u32, schema_lease_value[1..5], version, .little);
        const previous_lease = self.getAlloc(self.alloc, &schema_lease_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        defer if (previous_lease) |value| self.alloc.free(value);
        if (previous_lease) |value| if (extra_batch.schema_binding != null and !std.mem.eql(u8, value, &schema_lease_value))
            return error.SchemaInUse;

        if (record.source_purpose != null and record.prepared) {
            // A source PREPARE is one complete immutable final write set, not
            // an incremental session. Never re-evaluate its old read guards or
            // replace its retained rows after the durable vote.
            if (extra_batch.document_revisions) |revisions| if (revisions != record.document_revisions) return error.SourcePayloadConflict;
            try self.requireSamePreparedSourceIntents(txn_id, intents);
            if (extra_batch.writes.len != 0 or extra_batch.deletes.len != 0) return error.SourcePayloadConflict;
            return backend_erased.Ticket.alreadyDurable();
        }

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
        try self.checkIntentConflicts(extra_batch.graph_sources, txn_id);
        try self.checkIndexSpanGuardConflicts(extra_batch.index_span_digests, txn_id);

        var reservation_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer reservation_arena.deinit();
        var reserved_versions = std.StringHashMap(u64).init(reservation_arena.allocator());
        var reserved_children = std.StringHashMap(u64).init(reservation_arena.allocator());
        if (record.document_revisions or extra_batch.document_revision_scope != null) {
            var read = try self.store.beginProbe();
            defer read.abort();
            if (try document_revision_reservation.load(&read, txn_id)) |held| {
                const scope = extra_batch.document_revision_scope orelse return error.ExternalTransactionDecisionRequired;
                try self.requireDocumentRevisionScope(txn_id, scope);
                const owner = record.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
                if (!held.scope.eql(scope) or !(held.owner orelse return error.ExternalTransactionDecisionRequired).eql(owner) or !record.document_revisions or
                    (extra_batch.document_revisions != null and !extra_batch.document_revisions.?)) return error.ExternalTransactionDecisionChanged;
                const references = extra_batch.document_revision_references;
                if (references.len > document_revision_reservation.max_rows) return error.TransactionTooLarge;
                var demands = std.StringHashMap(u64).init(reservation_arena.allocator());
                var key_bytes: usize = 0;
                for (references, 0..) |reference, i| {
                    if (reference.key.len == 0 or reference.version == 0 or reference.version > document_revision_reservation.max_version or
                        (i != 0 and std.mem.order(u8, references[i - 1].key, reference.key) != .lt)) return error.InvalidTxnRecord;
                    key_bytes = std.math.add(usize, key_bytes, reference.key.len) catch return error.TransactionTooLarge;
                    if (key_bytes > document_revision_reservation.max_key_bytes) return error.TransactionTooLarge;
                    try demands.put(reference.key, reference.version);
                }
                for (intents) |intent| {
                    if (isRawMetadataIntentKey(intent.key)) continue;
                    const demand = demands.get(intent.key) orelse return error.ExternalTransactionDecisionChanged;
                    const key = try document_revision_reservation.rowKey(reservation_arena.allocator(), txn_id, intent.key);
                    const value = read.get(key) catch |err| return if (err == error.NotFound) error.ExternalTransactionDecisionChanged else err;
                    const assigned = try document_revision_reservation.rowVersion(value);
                    if (assigned != demand) return error.VersionConflict;
                    if (intent.prepared_row != null) return error.PreparedIntentRequiresMaterialization;
                    const timestamp_key = try internal_keys.ttlKeyAlloc(reservation_arena.allocator(), intent.key);
                    const current = read.get(timestamp_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (current) |bytes| if ((ttl.writeVersion(bytes) orelse return error.InvalidTimestamp) >= assigned) return error.VersionConflict;
                    try reserved_versions.put(intent.key, assigned);
                    try reserved_children.put(intent.key, try document_revision_reservation.rowSubtransaction(value));
                }
                if (reserved_versions.count() != references.len) return error.ExternalTransactionDecisionChanged;
            } else if (extra_batch.document_revision_scope != null) return error.ExternalTransactionDecisionChanged;
        }

        const source_language = blk: {
            var probe = try self.store.beginProbe();
            defer probe.abort();
            break :blk if (try retained_effects.load(&probe)) |state| state.ttl_sources else false;
        };
        const previous_admission = try self.loadIntentAdmission(self.alloc, txn_id);
        var admission = previous_admission orelse IntentAdmission{};
        if (first_prepare) if (extra_batch.prepared_enrichments_json) |packet| {
            admission.bytes = std.math.add(u64, admission.bytes, packet.len + prepared_enrichments_prefix.len + 16) catch return error.TransactionTooLarge;
        };
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
            var previous = try self.collectIntentBatch(self.alloc, txn_id);
            defer previous.deinit(self.alloc);
            for (previous.owned_entries.?) |entry| {
                const key = entry.key[intents_prefix.len + 17 ..];
                const decoded = try IntentValue.decode(entry.value);
                const previous_intent: WriteIntent = .{ .key = key, .value = decoded.value, .prepared_row = decoded.prepared_row, .row_policy_requirement = decoded.row_policy_requirement };
                const cost = try intentAdmissionBytes(previous_intent);
                const retained = try retainedIntentBytesForLanguage(self.alloc, previous_intent, source_language);
                admission.count += 1;
                admission.bytes = std.math.add(u64, admission.bytes, cost) catch return error.TransactionTooLarge;
                admission.retained_bytes = std.math.add(u64, admission.retained_bytes, retained) catch return error.TransactionTooLarge;
                admission.retained_keys += @intFromBool(retained != 0);
                try pending_costs.put(self.alloc, key, .{ .bytes = cost, .retained = retained, .retained_keys = @intFromBool(retained != 0) });
                const member_key = try makeIntentMemberKey(self.alloc, txn_id, key);
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
            const member_key = try makeIntentMemberKey(self.alloc, txn_id, intent.key);
            defer self.alloc.free(member_key);
            const prior = if (pending_costs.get(intent.key)) |cost| cost else blk: {
                const raw = self.getAlloc(self.alloc, member_key) catch |err| switch (err) {
                    error.NotFound => break :blk null,
                    else => return err,
                };
                defer self.alloc.free(raw);
                if (raw.len != 16 and raw.len != 24) return error.InvalidTxnRecord;
                break :blk IntentCost{ .bytes = std.mem.readInt(u64, raw[0..8], .little), .retained = std.mem.readInt(u64, raw[8..16], .little), .retained_keys = if (raw.len == 24) std.mem.readInt(u64, raw[16..24], .little) else @intFromBool(std.mem.readInt(u64, raw[8..16], .little) != 0) };
            };
            const envelope_bytes: u64 = if (reserved_versions.contains(intent.key)) 9 else 0;
            const cost = std.math.add(u64, try intentAdmissionBytes(intent), envelope_bytes) catch return error.TransactionTooLarge;
            const retained = try retainedIntentBytesForLanguage(self.alloc, intent, source_language);
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
        var range_reservations = std.ArrayListUnmanaged(VersionPredicate).empty;
        defer range_reservations.deinit(self.alloc);
        try range_reservations.appendSlice(self.alloc, predicates);
        var writer_keys: [range_protection.bucket_count][range_protection.writer_prefix.len + 2]u8 = undefined;
        var writer_buckets: std.StaticBitSet(range_protection.bucket_count) = .empty;
        const index_writer_keys = try self.alloc.alloc([range_protection.index_writer_prefix.len + range_protection.index_span_digest_bytes]u8, extra_batch.index_span_digests.len);
        defer self.alloc.free(index_writer_keys);
        {
            var probe = try self.store.beginProbe();
            defer probe.abort();
            const tracking_active = try range_protection.isActive(&probe);
            if (!tracking_active and extra_batch.index_span_digests.len != 0) return error.SqlRangeTrackingRequired;
            if (tracking_active) {
                for ([_][]const WriteIntent{ intents, extra_batch.graph_sources }) |sources| for (sources) |intent| {
                    if (std.mem.startsWith(u8, intent.key, "\x00\x00")) continue;
                    const id = range_protection.bucket(intent.key);
                    if (writer_buckets.isSet(id)) continue;
                    writer_buckets.set(id);
                    writer_keys[id] = range_protection.writerKey(id);
                    try range_reservations.append(self.alloc, .{ .key = &writer_keys[id], .expected_version = 0, .comparison = .exact_value, .expected_value = null });
                };
                for (extra_batch.index_span_digests, 0..) |digest, i| {
                    index_writer_keys[i] = range_protection.indexWriterKey(digest);
                    try range_reservations.append(self.alloc, .{ .key = &index_writer_keys[i], .expected_version = 0, .comparison = .exact_value, .expected_value = null });
                }
            }
        }
        const read_admission = try self.stageReadGuards(txn_id, range_reservations.items, &last_intents, &write_keys, &write_vals, &writes);
        const total_bytes = std.math.add(u64, admission.bytes, read_admission.next.bytes) catch return error.TransactionTooLarge;
        const previous_bytes = std.math.add(u64, if (previous_admission) |previous| previous.bytes else 0, read_admission.previous.bytes) catch return error.InvalidTxnRecord;
        if (extra_batch.max_intent_admission_bytes != 0 and total_bytes > extra_batch.max_intent_admission_bytes and
            total_bytes > previous_bytes)
            return error.TransactionTooLarge;

        for (intents, 0..) |intent, index| {
            if (last_intents.get(intent.key).? != index) continue;
            const member_key = try makeIntentMemberKey(self.alloc, txn_id, intent.key);
            try appendOwnedBytes(self.alloc, &write_keys, member_key);
            const member_value = try self.alloc.alloc(u8, 24);
            std.mem.writeInt(u64, member_value[0..8], pending_costs.get(intent.key).?.bytes, .little);
            std.mem.writeInt(u64, member_value[8..16], pending_costs.get(intent.key).?.retained, .little);
            std.mem.writeInt(u64, member_value[16..24], pending_costs.get(intent.key).?.retained_keys, .little);
            try appendOwnedBytes(self.alloc, &write_vals, member_value);
            try writes.append(self.alloc, .{ .key = member_key, .value = member_value });
            const intent_key = try self.makeIntentKey(txn_id, intent.key);
            try appendOwnedBytes(self.alloc, &write_keys, intent_key);

            // SQL provenance prefixes the existing immutable value envelope.
            // It is committed atomically with the prepare vote and survives
            // journal replay, restart and replacement of the same intent key.
            const base = try IntentValue.encode(self.alloc, intent);
            const val = if (reserved_versions.get(intent.key)) |revision| blk: {
                defer self.alloc.free(base);
                break :blk try IntentValue.withDocumentRevision(self.alloc, base, revision);
            } else base;
            try appendOwnedBytes(self.alloc, &write_vals, val);

            try writes.append(self.alloc, .{ .key = intent_key, .value = val });
            if (reserved_versions.get(intent.key)) |revision| {
                const receipt_key = try document_revision_reservation.rowKey(self.alloc, txn_id, intent.key);
                try appendOwnedBytes(self.alloc, &write_keys, receipt_key);
                const receipt_value = try document_revision_reservation.encodeRow(self.alloc, revision, reserved_children.get(intent.key).?, true);
                try appendOwnedBytes(self.alloc, &write_vals, receipt_value);
                try writes.append(self.alloc, .{ .key = receipt_key, .value = receipt_value });
            }

            const lock_key = try self.makeIntentLockKey(intent.key);
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
        record.publication_capture_required = capturing;
        record.replication_source_required = extra_batch.replication_prepare != null;
        // Pin the scheme with the first vote. A transaction may span a
        // rolling activation; later prepares preserve its durable contract.
        // Records predating the prepare bit remain conservatively legacy.
        if (first_prepare) if (extra_batch.document_revisions) |enabled| {
            record.document_revisions = enabled;
        };
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

        // The embedded reader fence is irreversible and committed with this
        // exact vote. Older DB.open decoders reject ATBL5 before recovering or
        // serving a root containing graph-bearing intent envelopes.
        var graph_catalog: ?@import("db/table_catalog.zig").Encoded = null;
        for (intents) |intent| if (intent.prepared_graph_effects != null) {
            const table_catalog = @import("db/table_catalog.zig");
            const current_bytes = self.getAlloc(self.alloc, table_catalog.key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (current_bytes) |bytes| self.alloc.free(bytes);
            var current = if (current_bytes) |bytes| try table_catalog.Catalog.decode(bytes) else table_catalog.Catalog{};
            for (extra_batch.writes) |write| if (std.mem.eql(u8, write.key, table_catalog.key)) return error.InvalidTxnRecord;
            if (current.transaction_effect_format == 0) {
                current.transaction_effect_format = 1;
                graph_catalog = try current.encodeDurable();
                try writes.append(self.alloc, .{ .key = table_catalog.key, .value = graph_catalog.?.slice() });
            }
            break;
        };

        const prepared_key = makeSidecarKey(prepared_enrichments_prefix, txn_id);
        if (extra_batch.prepared_enrichments_json) |packet| {
            const previous = self.getAlloc(self.alloc, &prepared_key) catch |err| if (err == error.NotFound) null else return err;
            defer if (previous) |bytes| self.alloc.free(bytes);
            if (previous) |bytes| {
                if (!std.mem.eql(u8, bytes, packet)) return error.VersionConflict;
            } else {
                if (!first_prepare) return error.InvalidTxnRecord;
                try writes.append(self.alloc, .{ .key = &prepared_key, .value = packet });
            }
        }
        try writes.appendSlice(self.alloc, extra_batch.writes);

        const ticket = try self.applyBatchStaged(writes.items, extra_batch.deletes, null, .{
            .previous = if (previous_admission) |old| old.reservation() else 0,
            .next = admission.reservation(),
            .capture = if (extra_batch.publication_prepare_sink) |sink| .{ .id = txn_id, .sink = sink } else null,
            .publication = .{ .source = extra_batch.replication_prepare, .input = extra_batch.publication_capture, .id = txn_id, .revision = record.intent_revision, .intents = intents },
        }, null);

        self.traceWriteIntentSuccess(txn_id, intents, predicates);
        return ticket;
    }

    /// Replicated phase-one visibility barrier for an externally committed
    /// transaction. The metadata decision owner activates every participant
    /// before resolving the first. Existing coordinated read fences then see
    /// an old immutable cut, a retryable barrier, or fully committed rows.
    pub fn activateExternalDecision(self: *TxnManager, txn_id: TxnId, owner: ExternalDecisionOwner, extra: MutationExtraBatch) !void {
        try self.requireNativeMutationPin(extra.native_pin);
        try owner.validate();
        var record = try self.loadTransactionRecord(txn_id);
        if (record.replication_source_required != (extra.replication_activation != null)) return error.ReplicationCutoverAuthorityLost;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const activation = if (extra.replication_activation) |phase| blk: {
            const permit = phase.permit;
            var read = try self.store.beginRead();
            defer read.abort();
            const namespace = @import("db/online_source_contract.zig").namespaceBytes(permit.namespace);
            _ = try @import("source_authority.zig").require(&read, .raft, namespace);
            const prepared = (try source_apply.preparedForTransaction(arena.allocator(), &read, txn_id, owner, permit.namespace)) orelse return error.ReplicationCutoverAuthorityLost;
            if (!std.meta.eql(prepared.receipt.permit, permit) or prepared.intent_revision != record.intent_revision)
                return error.ReplicationCutoverAuthorityLost;
            break :blk try source_apply.activationWrite(arena.allocator(), &read, prepared, phase.position, phase.physical_root);
        } else null;
        const held = record.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
        if (!held.eql(owner)) return error.ExternalTransactionDecisionChanged;
        if (!record.prepared or record.status == .aborted) return error.DecisionConflict;
        var barrier_value: [8]u8 = undefined;
        const install_barrier = !record.external_commit_started;
        if (extra.replication_activation != null and install_barrier != (activation != null))
            return error.ReplicationCutoverAuthorityLost;
        if (install_barrier) {
            var read = try self.store.beginProbe();
            defer read.abort();
            const count = std.math.add(u64, try externalReadBarrierCount(&read), 1) catch return error.InvalidTxnRecord;
            std.mem.writeInt(u64, &barrier_value, count, .little);
        }
        record.external_commit_started = true;
        const encoded = try self.encodeRecord(record);
        defer self.alloc.free(encoded);
        const key = makeRecordKey(txn_id);
        var writes: std.ArrayList(docstore.KVPair) = .empty;
        defer writes.deinit(self.alloc);
        try writes.append(self.alloc, .{ .key = &key, .value = encoded });
        if (install_barrier) try writes.append(self.alloc, .{ .key = external_read_barrier_count_key, .value = &barrier_value });
        if (activation) |write| try writes.append(self.alloc, write);
        try writes.appendSlice(self.alloc, extra.writes);
        try self.applyBatch(writes.items, extra.deletes, null);
    }

    fn externalExpiryFloor(self: *TxnManager) !u64 {
        var read = try self.store.beginProbe();
        defer read.abort();
        return readExternalExpiryFloor(&read);
    }

    fn readExternalExpiryFloor(read: anytype) !u64 {
        const bytes = read.get(external_expiry_floor_key) catch |err| switch (err) {
            error.NotFound => return 0,
            else => return err,
        };
        if (bytes.len != 8) return error.InvalidTxnRecord;
        const floor = std.mem.readInt(u64, bytes[0..8], .little);
        if (floor == 0) return error.InvalidTxnRecord;
        return floor;
    }

    fn fenceMissingExternalAbort(self: *TxnManager, id: TxnId, timestamp: u64, extra: ResolutionExtraBatch) !StagedResolutionOutcome {
        const owner = extra.external_decision_owner.?;
        try owner.validate();
        const source = extra.replication_resolution;
        if (source) |input| try requireReplicationResolution(id, owner, input);
        if (source == null and try self.externalExpiryFloor() >= owner.expires_at_unix_ns) {
            const ticket = if (extra.completion_writes.len != 0 or extra.completion_deletes.len != 0)
                try self.applyBatchStaged(extra.completion_writes, extra.completion_deletes, null, null, null)
            else
                null;
            return .{ .outcome = .{ .applied = false, .replay_sequence = 0 }, .ticket = ticket };
        }
        const key = makeRecordKey(id);
        const record: TxnRecord = .{ .status = .aborted, .begin_timestamp = timestamp, .commit_version = 0, .created_at = timestamp, .finalized_at = timestamp, .retain_terminal = true, .intents_resolved = true, .intents_resolved_known = true, .external_decision_owner = owner, .native_pin = extra.native_pin orelse self.native_relation_pin, .replication_source_required = source != null };
        const bytes = try self.encodeRecord(record);
        defer self.alloc.free(bytes);
        var writes: std.ArrayListUnmanaged(docstore.KVPair) = .empty;
        defer writes.deinit(self.alloc);
        try writes.append(self.alloc, .{ .key = &key, .value = bytes });
        const participant_key = makeSidecarKey(participants_prefix, id);
        const resolved_key = makeSidecarKey(resolved_participants_prefix, id);
        const participants = if (source == null and extra.resolved_participant != null)
            try encodeParticipantList(self.alloc, &.{extra.resolved_participant.?})
        else
            null;
        defer if (participants) |value| self.alloc.free(value);
        if (participants) |value| {
            try writes.append(self.alloc, .{ .key = &participant_key, .value = value });
            // Native metadata-owned resolution is not the durable metadata
            // handoff ACK. In particular ABORT may precede BEGIN and its reply
            // may be lost after admission expiry. Keep the actual local roster
            // unresolved until the existing explicit external ACK arrives.
            if (record.native_pin == null and source == null)
                try writes.append(self.alloc, .{ .key = &resolved_key, .value = value });
        }
        try writes.appendSlice(self.alloc, extra.completion_writes);
        const reservation: ?ReservationChange = if (source) |input| .{
            .previous = 0,
            .next = 0,
            .source_resolution = .{ .input = input, .revision = 0, .status = .aborted },
        } else null;
        const ticket = try self.applyBatchStaged(writes.items, extra.completion_deletes, null, reservation, null);
        return .{ .outcome = .{ .applied = false, .replay_sequence = 0 }, .ticket = ticket };
    }

    fn requireReplicationResolution(id: TxnId, owner: ExternalDecisionOwner, input: ReplicationOrderedPhase) !void {
        try input.permit.validate();
        try input.position.validate();
        if (input.position != .raft or input.physical_root == 0 or !owner.eql(input.permit.decision_owner) or
            !std.mem.eql(u8, &id, &input.permit.reference.txn_id)) return error.ReplicationCutoverAuthorityLost;
    }

    /// Cancel owner reservations when the coordinator aborts before BEGIN,
    /// retaining the terminal decision for durable participant acknowledgement.
    fn fenceReservedExternalAbort(self: *TxnManager, id: TxnId, timestamp: u64, previous: TxnRecord, extra: ResolutionExtraBatch) !StagedResolutionOutcome {
        const owner = extra.external_decision_owner.?;
        try owner.validate();
        if (previous.status != .pending or previous.prepared or previous.intent_revision != 0 or previous.coordinator or previous.retain_terminal or previous.external_commit_started) return error.ExternalTransactionDecisionChanged;
        // The apply fence makes tip probes and this prefix cursor one cut.
        // Do not clone the entire mutable memtable merely to cancel own keys.
        const View = struct {
            probe: backend_erased.ProbeTxn,
            scan: backend_erased.CurrentScanTxn,
            pub fn get(view: *@This(), key: []const u8) ![]const u8 {
                return view.probe.get(key);
            }
            pub fn getManySorted(view: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
                return view.probe.getManySorted(keys, values);
            }
            pub fn openCursor(view: *@This()) !backend_erased.Cursor {
                return view.scan.openCursor();
            }
        };
        var probe = try self.store.beginProbe();
        defer probe.abort();
        var scan = try self.store.beginCurrentScan();
        defer scan.abort();
        var read: View = .{ .probe = probe, .scan = scan };
        var header = (try document_revision_reservation.load(&read, id)) orelse return error.InvalidTxnRecord;
        try self.requireDocumentRevisionScope(id, header.scope);
        if (header.owner != null or !std.mem.eql(u8, &header.scope.metadata_incarnation, &owner.metadata_incarnation)) return error.ExternalTransactionDecisionChanged;
        const peer = extra.resolved_participant orelse return error.InvalidParticipant;
        const peers = try self.getParticipants(self.alloc, id);
        defer freeParticipantList(self.alloc, peers);
        if (peers.len != 0) return error.ExternalTransactionDecisionChanged;
        var plan = (try self.prepareAbandonDocumentRevisions(&read, id, header.scope.principal_digest, timestamp)) orelse return error.ExternalTransactionDecisionChanged;
        defer plan.deinit();
        var record = previous;
        record.status = .aborted;
        record.finalized_at = timestamp;
        record.retain_terminal = true;
        record.retain_terminal_known = true;
        record.external_decision_owner = owner;
        record.native_pin = extra.native_pin orelse self.native_relation_pin;
        record.intents_resolved = true;
        record.intents_resolved_known = true;
        const encoded = try self.encodeRecord(record);
        defer self.alloc.free(encoded);
        try plan.put(&makeRecordKey(id), encoded);
        header.owner = owner;
        header.count = 0;
        header.key_bytes = 0;
        try plan.put(&document_revision_reservation.headerKey(id), &try header.encode());
        const roster = try encodeParticipantList(self.alloc, &.{peer});
        defer self.alloc.free(roster);
        try plan.put(&makeSidecarKey(participants_prefix, id), roster);
        try plan.put(&makeParticipantIndexKey(participant_index_prefix, id, peer), peer);
        var indexed: [17]u8 = undefined;
        (ParticipantIndex{ .enlisted = 1, .resolved = if (record.native_pin == null) 1 else 0 }).encode(&indexed);
        try plan.put(&makeSidecarKey(participant_index_prefix, id), &indexed);
        if (record.native_pin == null) {
            try plan.put(&makeSidecarKey(resolved_participants_prefix, id), roster);
            try plan.put(&makeParticipantIndexKey(resolved_index_prefix, id, peer), peer);
        }
        for (extra.completion_writes) |write| try plan.put(write.key, write.value);
        for (extra.completion_deletes) |key| try plan.erase(key);
        const ticket = try self.applyBatchStaged(plan.writes.items, plan.deletes.items, null, null, extra.commit_participant);
        return .{ .outcome = .{ .applied = false, .replay_sequence = 0 }, .ticket = ticket };
    }

    /// Trusted ordered retirement after an independent qualified coordinator
    /// read. The DB caller must hold the physical owner's apply fence throughout.
    /// Missing local BEGIN fences the bounded scope floor; it never fabricates
    /// a local transaction outcome or allocates per-attempt tombstones.
    pub fn retireScopedSourceAttempt(self: *TxnManager, command: @import("source_scope_retirement.zig").Command, applied: source_scope.Cut, marker_writes: []const docstore.KVPair) !@import("source_scope_retirement.zig").Reply {
        const retirement = @import("source_scope_retirement.zig");
        const proof = try command.proof();
        if (applied.term == 0 or applied.index == 0) return error.InvalidSourceAdmission;
        const namespace = try self.getAlloc(self.alloc, &internal_keys.identity_namespace_key);
        defer self.alloc.free(namespace);
        if (!std.mem.eql(u8, namespace, &proof.root.namespace)) return error.HistoryResynchronizationRequired;
        const existing = self.loadTransactionRecord(proof.slot.transaction_id) catch |err| switch (err) {
            TxnError.TxnNotFound => null,
            else => return err,
        };
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        if (existing) |record| {
            if (!sameSourceScope(record.source_scope, proof.slot.reference) or !samePurpose(record.source_purpose, proof.slot.source_purpose) or record.begin_timestamp != command.observation.decision.?.record.begin_timestamp) return error.SourceTransactionIdentityConflict;
            if (record.status == .committed) return error.DecisionConflict;
            var admission = (try self.getCompletionAdmission(self.alloc, proof.slot.transaction_id)) orelse return error.SourceOutcomeUnresolved;
            defer admission.deinit();
            if (!std.mem.eql(u8, &admission.value.digest, &proof.slot.admission_digest)) return error.SourceTransactionIdentityConflict;
            const member = try admission.value.localMember(proof.root.namespace);
            if (!std.mem.eql(u8, &member.owner_fence, &command.intent.owner.digest())) return error.InvalidParticipant;
            _ = try self.resolveIntentsWithExtraBatch(proof.slot.transaction_id, .aborted, command.observation.decision.?.record.finalized_at_ns, .{
                .completion_owner = command.intent.owner,
                .source_purpose = proof.slot.source_purpose,
                .source_scope = proof.slot.reference,
                .source_scope_cut = applied,
                .source_retirement = .{ .command = command, .cut = applied, .already_fenced = record.status != .pending and record.intents_resolved_known and record.intents_resolved },
                .resolved_participant = member.locator,
                .completion_writes = marker_writes,
            });
            var probe = try self.store.beginProbe();
            defer probe.abort();
            return switch (try @import("source_scope_retirement_ordered.zig").replay(arena.allocator(), &probe, command, applied)) {
                .applied => |reply| reply,
                .rejected, .superseded => error.SourceOutcomeUnresolved,
            };
        }
        var batch = try self.store.beginBatch();
        errdefer batch.abort();
        const reply = try retirement.applyUnenlisted(arena.allocator(), &batch, command, applied);
        try @import("source_scope_retirement_ordered.zig").stage(arena.allocator(), &batch, command, reply);
        for (marker_writes) |write| try batch.put(write.key, write.value);
        try batch.commit();
        return reply;
    }

    /// Resolve intents: commit applies them to real keys, abort deletes them.
    /// Conflicting terminal decisions return `TxnError.DecisionConflict`; callers
    /// should treat that as a protocol inconsistency / torn-state signal rather
    /// than a retryable OCC conflict.
    fn validateReservedMaterialization(self: *TxnManager, a: Allocator, entries: []const backend_scan.OwnedKVPair, extra: ResolutionExtraBatch) !void {
        const Expected = struct { version: u64, write: bool };
        var expected = std.StringHashMap(Expected).init(a);
        var highest: u64 = 0;
        for (entries) |entry| {
            const key = entry.key[intents_prefix.len + 17 ..];
            if (isRawMetadataIntentKey(key)) continue;
            const intent = try IntentValue.decode(entry.value);
            const version = intent.document_revision orelse return error.InvalidTxnRecord;
            highest = @max(highest, version);
            const timestamp_key = try internal_keys.ttlKeyAlloc(a, key);
            try expected.put(timestamp_key, .{ .version = version, .write = intent.value != null });
        }
        var read = try self.store.beginProbe();
        defer read.abort();
        const raw_floor = try read.get(ttl.document_version_watermark_key);
        if (raw_floor.len != 8) return error.InvalidTimestamp;
        const floor = std.mem.readInt(u64, raw_floor[0..8], .little);
        if (floor < highest) return error.InvalidTimestamp;
        inline for (.{ extra.writes, extra.completion_writes }) |writes| for (writes) |write| {
            if (std.mem.eql(u8, write.key, ttl.document_version_watermark_key)) {
                if (write.value.len != 8 or std.mem.readInt(u64, write.value[0..8], .little) < floor) return error.InvalidTimestamp;
            } else if (internal_keys.isTtlKey(write.key)) {
                const held = expected.get(write.key) orelse return error.InvalidTxnRecord;
                if (!held.write or (ttl.writeVersion(write.value) orelse return error.InvalidTimestamp) != held.version) return error.InvalidTxnRecord;
            }
        };
        inline for (.{ extra.deletes, extra.completion_deletes }) |deletes| for (deletes) |key| {
            if (std.mem.eql(u8, key, ttl.document_version_watermark_key)) return error.InvalidTimestamp;
            if (internal_keys.isTtlKey(key)) if (expected.get(key)) |held| if (held.write) return error.InvalidTxnRecord;
        };
    }

    pub fn resolveIntents(self: *TxnManager, txn_id: TxnId, status: TxnStatus, timestamp: u64) !void {
        _ = try self.resolveIntentsWithExtraBatch(txn_id, status, timestamp, .{});
    }

    /// Resume only an original, already durable terminal decision after
    /// producer closure. Callers cannot choose COMMIT/ABORT or its version.
    /// Pending coordinator decisions require a separate qualified protocol.
    pub fn resolveClosedLegacyTerminal(self: *TxnManager, proof: LegacyTerminalResolution, extra: ResolutionExtraBatch) !ResolutionOutcome {
        if (extra.source_retirement != null or extra.source_scope != null or extra.source_purpose != null) return error.InvalidSourceAdmission;
        const decision = try self.closedLegacyTerminalDecision(proof);
        const staged = try self.resolveIntentsWithExtraBatchStagedAndLegacy(proof.transaction_id, decision.status, decision.timestamp, extra, proof);
        if (staged.ticket) |ticket| try ticket.wait();
        return staged.outcome;
    }

    pub const LegacyTerminalDecision = struct { status: TxnStatus, timestamp: u64 };
    pub fn closedLegacyTerminalDecision(self: *TxnManager, proof: LegacyTerminalResolution) !LegacyTerminalDecision {
        try proof.validate();
        var read = try self.store.beginRead();
        defer read.abort();
        _ = (try @import("source_producer_policy.zig").Store.read(self.alloc, &read, proof.root)) orelse return error.SourcePendingWork;
        const raw = read.get(&makeRecordKey(proof.transaction_id)) catch |err| return if (err == error.NotFound) error.TxnNotFound else err;
        if (!std.mem.eql(u8, &completion.bytesDigest(raw), &proof.record_digest)) return error.SourceTransactionIdentityConflict;
        const record = try decodeRecord(raw);
        if (record.status == .pending or record.ordinary_producer or record.source_scope != null or record.finalized_at == 0 or (record.status == .committed and record.commit_version == 0)) return error.SourceOutcomeUnresolved;
        return .{ .status = record.status, .timestamp = if (record.status == .committed) record.commit_version else record.finalized_at };
    }

    /// Blocking convenience wrapper over `resolveIntentsWithExtraBatchStaged`:
    /// identical behavior to before that split existed (every caller here
    /// still gets a fully durable outcome before this returns). Callers on
    /// the synchronous write ack path that need to release locks between
    /// staging and durability (lite-single-commit-write-path.md §3.2/§3.2a)
    /// should call the staged form directly instead.
    pub fn resolveIntentsWithExtraBatch(
        self: *TxnManager,
        txn_id: TxnId,
        status: TxnStatus,
        timestamp: u64,
        extra_batch: ResolutionExtraBatch,
    ) !ResolutionOutcome {
        // The tag must wrap `wait()` too: the actual apply/fsync happens
        // there, not at `commitStaged`/`enqueue` time (see
        // `initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch`'s
        // comment above).
        const wal_trace_scope = wal_trace.scopeDefault(.relational_intent_resolve);
        defer wal_trace_scope.end();
        const staged = try self.resolveIntentsWithExtraBatchStaged(txn_id, status, timestamp, extra_batch);
        if (staged.ticket) |ticket| try ticket.wait();
        return staged.outcome;
    }

    /// Staged counterpart: stages every durable effect of this resolution
    /// attempt (the transaction record transition, applied intents, and any
    /// completion writes) and returns a ticket instead of blocking for the
    /// fsync, mirroring `docstore.zig`'s `Batch.commitStaged`/`StagedBatch`
    /// split. `ticket` is `null` only when this call wrote nothing at all
    /// (the no-op terminal-retry branch below with no completion writes or
    /// deletes) -- there is then nothing to wait for.
    pub fn resolveIntentsWithExtraBatchStaged(
        self: *TxnManager,
        txn_id: TxnId,
        status: TxnStatus,
        timestamp: u64,
        extra_batch: ResolutionExtraBatch,
    ) !StagedResolutionOutcome {
        return self.resolveIntentsWithExtraBatchStagedAndLegacy(txn_id, status, timestamp, extra_batch, null);
    }

    fn completionResolutionParticipant(self: *TxnManager, id: TxnId, record: TxnRecord, owner: ?completion.RoutedOwner, requested: []const u8) !?[]u8 {
        if (!record.coordinator_known or record.coordinator) return null;
        const binding = owner orelse return null;
        var admission = (try self.getCompletionAdmission(self.alloc, id)) orelse return null;
        defer admission.deinit();
        if (admission.value.admission.version < 3) return null;
        try admission.value.admission.requireRoutedOwner(binding);
        const namespace = try self.getAlloc(self.alloc, &internal_keys.identity_namespace_key);
        defer self.alloc.free(namespace);
        if (!std.mem.eql(u8, namespace, &binding.namespace)) return error.InvalidParticipant;
        const local = try admission.value.localMember(binding.namespace);
        if (!std.mem.eql(u8, &local.owner_fence, &binding.digest())) return error.InvalidParticipant;
        if (!std.mem.eql(u8, requested, local.locator)) return null;
        const coordinator = try admission.value.admission.coordinatorMember();
        const participants = try self.getParticipants(self.alloc, id);
        defer freeParticipantList(self.alloc, participants);
        if (participants.len == 1 and std.mem.eql(u8, participants[0], coordinator.locator)) return try self.alloc.dupe(u8, coordinator.locator);
        return null;
    }

    pub fn resolveIntentsWithExtraBatchStagedAndLegacy(
        self: *TxnManager,
        txn_id: TxnId,
        status: TxnStatus,
        timestamp: u64,
        extra_batch: ResolutionExtraBatch,
        legacy_terminal: ?LegacyTerminalResolution,
    ) !StagedResolutionOutcome {
        if (legacy_terminal != null and (extra_batch.source_retirement != null or extra_batch.source_scope != null or extra_batch.source_purpose != null)) return error.InvalidSourceAdmission;
        if (extra_batch.source_retirement) |ordered| {
            const proof = try ordered.command.proof();
            if (status != .aborted or !std.mem.eql(u8, &txn_id, &proof.slot.transaction_id) or !sameSourceScope(extra_batch.source_scope, proof.slot.reference) or !samePurpose(extra_batch.source_purpose, proof.slot.source_purpose) or extra_batch.completion_owner == null or !std.meta.eql(extra_batch.completion_owner.?, ordered.command.intent.owner) or extra_batch.source_scope_cut == null or !std.meta.eql(extra_batch.source_scope_cut.?, ordered.cut)) return error.SourceTransactionIdentityConflict;
        }
        try self.requireNativeMutationPin(extra_batch.native_pin);
        const rec_key = makeRecordKey(txn_id);
        const completion_request_key = completion.requestKey(txn_id);
        var completion_request: [124]u8 = undefined;
        const schema_lease_key = makeSidecarKey(schema_leases_prefix, txn_id);
        var record = self.loadTransactionRecord(txn_id) catch |err| {
            if (err == error.TxnNotFound and status == .aborted and extra_batch.external_decision_owner != null and extra_batch.publication_terminal_sink == null)
                return self.fenceMissingExternalAbort(txn_id, timestamp, extra_batch);
            return err;
        };
        if (record.replication_source_required and extra_batch.replication_resolution == null) return error.ReplicationCutoverAuthorityLost;
        if (extra_batch.publication_terminal_sink != null and !record.publication_capture_required)
            return error.LogicalCaptureReceiptMissing;
        var terminal_region: std.heap.ArenaAllocator = .init(self.alloc);
        defer terminal_region.deinit();
        const replication_prepared = if (extra_batch.replication_resolution) |resolution| prepared: {
            const a = terminal_region.allocator();
            const owner = record.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
            try requireReplicationResolution(txn_id, owner, resolution);
            if (!(extra_batch.external_decision_owner orelse return error.ExternalTransactionDecisionRequired).eql(owner))
                return error.ExternalTransactionDecisionChanged;
            var read = try self.store.beginRead();
            defer read.abort();
            if (!record.prepared) {
                if (status != .aborted or record.intent_revision != 0 or record.native_pin != null) return error.ReplicationCutoverAuthorityLost;
                if (record.status != .pending) {
                    if (!record.replication_source_required or record.status != .aborted) return error.ReplicationCutoverAuthorityLost;
                    const receipt = (try source_apply.readAborted(a, &read, resolution.permit)) orelse return error.ReplicationCutoverAuthorityLost;
                    if (receipt.image.physical_root_incarnation != resolution.physical_root or
                        (!std.meta.eql(receipt.position, resolution.position) and !resolution.position.strictlyAfter(receipt.position)))
                        return error.ReplicationCutoverAuthorityLost;
                }
                record.replication_source_required = true;
                break :prepared null;
            }
            if (!record.replication_source_required or status == .pending) return error.ReplicationCutoverAuthorityLost;
            break :prepared try source_apply.requirePrepared(a, &read, resolution.permit, resolution.physical_root, record.intent_revision);
        } else null;
        var ordered_capture: @import("db/logical_replication_outbox.zig").OrderedResolution = undefined;
        var publication_terminal_sink = extra_batch.publication_terminal_sink;
        if (record.publication_capture_required and !record.intents_resolved and publication_terminal_sink == null) {
            const position = extra_batch.publication_ordered_position orelse return error.LogicalCaptureReceiptMissing;
            const owner = extra_batch.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
            if (!owner.eql(record.external_decision_owner orelse return error.InvalidTxnRecord)) return error.ExternalTransactionDecisionChanged;
            ordered_capture = .{
                .alloc = terminal_region.allocator(),
                .position = position,
                .owner = owner,
                .transaction_id = txn_id,
                .intent_revision = record.intent_revision,
                .status = switch (status) {
                    .committed => .committed,
                    .aborted => .aborted,
                    .pending => return error.InvalidTxnRecord,
                },
                .commit_version = timestamp,
            };
            publication_terminal_sink = ordered_capture.sink();
        }
        if (status == .aborted and record.document_revisions and record.external_decision_owner == null and extra_batch.external_decision_owner != null)
            return self.fenceReservedExternalAbort(txn_id, timestamp, record, extra_batch);
        const retain_metadata_ack = (record.native_pin != null or record.publication_capture_required or record.replication_source_required) and record.external_decision_owner != null;
        if (record.status == .pending or extra_batch.source_purpose != null or extra_batch.source_scope != null or record.source_scope != null) {
            if (!samePurpose(record.source_purpose, extra_batch.source_purpose) or !sameSourceScope(record.source_scope, extra_batch.source_scope)) return error.SourceTransactionIdentityConflict;
        }
        if (record.status == .pending and status == .committed and (!record.prepared_known or !record.prepared)) {
            const plan_key = makeSidecarKey(completion_admission_prefix, txn_id);
            if (record.source_purpose != null or try self.keyExists(&plan_key)) return error.SourceOutcomeUnresolved;
        }
        var resolved_participant_key: [resolved_participants_prefix.len + 16]u8 = undefined;
        var resolved_participant_value: ?[]u8 = null;
        defer if (resolved_participant_value) |value| self.alloc.free(value);
        var indexed_ack: IndexedResolutionAck = .{};
        var owned_participant: ?[]u8 = null;
        defer if (owned_participant) |copy| self.alloc.free(copy);
        if (extra_batch.resolved_participant) |requested| {
            owned_participant = try self.completionResolutionParticipant(txn_id, record, extra_batch.completion_owner, requested);
            const participant = owned_participant orelse requested;
            if (!try indexed_ack.prepare(self, txn_id, participant)) {
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
        if (record.external_decision_owner) |owner| {
            if (extra_batch.external_decision_owner) |requested| {
                try requested.validate();
                if (!owner.eql(requested)) return error.ExternalTransactionDecisionChanged;
            } else if (!was_terminal) return error.ExternalTransactionDecisionRequired;
            if (!was_terminal and status == .committed and !record.external_commit_started) return error.ExternalTransactionCommitNotActivated;
            if (!was_terminal and status == .aborted and record.external_commit_started) return error.DecisionConflict;
        } else if (extra_batch.external_decision_owner != null) return error.ExternalTransactionDecisionChanged;
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
        if (was_terminal and record.intents_resolved_known and record.intents_resolved) {
            var completion_writes = std.ArrayListUnmanaged(docstore.KVPair).empty;
            defer completion_writes.deinit(self.alloc);
            try completion_writes.appendSlice(self.alloc, extra_batch.completion_writes);
            if (!retain_metadata_ack) try indexed_ack.append(self.alloc, &completion_writes);
            if (!retain_metadata_ack) if (resolved_participant_value) |value| {
                try completion_writes.append(self.alloc, .{ .key = &resolved_participant_key, .value = value });
            };
            const ticket: ?backend_erased.Ticket = if (completion_writes.items.len != 0 or extra_batch.completion_deletes.len != 0 or extra_batch.source_retirement != null or legacy_terminal != null)
                try self.applyBatchStagedWithHistory(completion_writes.items, extra_batch.completion_deletes, null, null, null, null, extra_batch.source_retirement, legacy_terminal)
            else
                null;
            return .{ .outcome = .{
                .applied = false,
                .replay_sequence = record.replay_sequence,
            }, .ticket = ticket };
        }

        // Scan all intents for this txn
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        if (extra_batch.captured_intents != null and extra_batch.expected_intent_revision == null) return error.InvalidArgument;
        const intent_entries = if (extra_batch.captured_intents) |entries| entries else if (extra_batch.known_intent_keys) |keys|
            if (!record.replication_source_required and extra_batch.skip_all_intent_application and extra_batch.expected_intent_revision != null and extra_batch.prepared_graph_effects_checked)
                try intentRetirementEntries(self.alloc, scan_prefix, keys)
            else
                try self.loadIntentEntriesByKeys(self.alloc, scan_prefix, keys)
        else if (status == .aborted)
            try self.loadIntentRetirementEntries(self.alloc, txn_id, scan_prefix)
        else
            try self.loadIntentEntries(self.alloc, txn_id, scan_prefix);
        defer if (extra_batch.captured_intents == null) backend_scan.freeResults(self.alloc, intent_entries);

        if (status == .committed) if (replication_prepared) |prepared| {
            const a = terminal_region.allocator();
            const frozen = try a.alloc(source_apply.FrozenIntent, intent_entries.len);
            for (intent_entries, frozen) |entry, *item|
                item.* = .{ .key = entry.key[scan_prefix.len..], .encoded_value = entry.value };
            try source_apply.validatePreparedIntents(a, prepared, frozen);
        };
        const prepared_key = makeSidecarKey(prepared_enrichments_prefix, txn_id);
        const read_prefix = makeSidecarKey(read_members_prefix, txn_id);
        const read_admission = try self.loadReadAdmission(txn_id);
        const read_entries: []backend_scan.OwnedKVPair = if (read_admission.count != 0) try self.scanPrefix(self.alloc, &read_prefix) else &.{};
        defer backend_scan.freeResults(self.alloc, read_entries);
        if (read_entries.len != read_admission.count) return error.InvalidTxnRecord;

        // A resolve retry after the terminal record and all intents are already
        // durable must not apply the caller's derived batch again. In
        // particular, doing so could overwrite a newer user write with the
        // transaction's old value.
        if (was_terminal and intent_entries.len == 0 and read_entries.len == 0) {
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
            if (!retain_metadata_ack) try indexed_ack.append(self.alloc, &completion_writes);
            if (!retain_metadata_ack) if (resolved_participant_value) |value| {
                try completion_writes.append(self.alloc, .{ .key = &resolved_participant_key, .value = value });
            };
            var completion_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer completion_deletes.deinit(self.alloc);
            try completion_deletes.append(self.alloc, &stale_manifest_key);
            try completion_deletes.append(self.alloc, &stale_admission_key);
            try completion_deletes.append(self.alloc, &schema_lease_key);
            try completion_deletes.append(self.alloc, &prepared_key);
            try completion_deletes.appendSlice(self.alloc, extra_batch.completion_deletes);
            const admission = try self.loadIntentAdmission(self.alloc, txn_id);
            const ticket = try self.applyBatchStagedWithHistory(completion_writes.items, completion_deletes.items, null, .{ .previous = if (admission) |value| value.reservation() else 0, .next = 0 }, null, extra_batch.source_scope_cut, extra_batch.source_retirement, legacy_terminal);
            return .{ .outcome = .{
                .applied = false,
                .replay_sequence = record.replay_sequence,
            }, .ticket = ticket };
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
        try deletes.append(self.alloc, &prepared_key);
        var owned_apply_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_apply_keys.items) |key| self.alloc.free(key);
            owned_apply_keys.deinit(self.alloc);
        }

        var external_barrier_value: [8]u8 = undefined;
        if (!was_terminal and record.external_decision_owner != null and record.external_commit_started) {
            var read = try self.store.beginProbe();
            defer read.abort();
            const remaining = std.math.sub(u64, try externalReadBarrierCount(&read), 1) catch return error.InvalidTxnRecord;
            if (remaining == 0) {
                try deletes.append(self.alloc, external_read_barrier_count_key);
            } else {
                std.mem.writeInt(u64, &external_barrier_value, remaining, .little);
                try writes.append(self.alloc, .{ .key = external_read_barrier_count_key, .value = &external_barrier_value });
            }
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
            try owned_apply_keys.append(self.alloc, member_key);
            try deletes.append(self.alloc, member_key);
            const lock_key = try self.makeIntentLockKey(user_key);
            try owned_apply_keys.append(self.alloc, lock_key);
            try deletes.append(self.alloc, lock_key);
        }
        const intent_keys_key = makeSidecarKey(intent_keys_prefix, txn_id);
        const admission_key = makeSidecarKey(intent_admission_prefix, txn_id);
        try deletes.append(self.alloc, &admission_key);
        try deletes.append(self.alloc, &intent_keys_key);
        try deletes.append(self.alloc, &schema_lease_key);
        var revision_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer revision_arena.deinit();
        const revision_alloc = revision_arena.allocator();
        var resolved_version = timestamp;
        const reservation: ?document_revision_reservation.Header = if (record.document_revisions) blk: {
            var read = try self.store.beginProbe();
            defer read.abort();
            break :blk try document_revision_reservation.load(&read, txn_id);
        } else null;
        if (reservation) |held| {
            if (status == .committed and (extra_batch.skip_all_intent_application or extra_batch.skip_intent_keys.len != 0)) return error.PreparedIntentRequiresMaterialization;
            if (record.external_decision_owner) |owner| {
                if (!(held.owner orelse return error.ExternalTransactionDecisionRequired).eql(owner)) return error.InvalidTxnRecord;
            } else if (held.owner != null or status != .aborted or record.prepared or record.intent_revision != 0) return error.ExternalTransactionDecisionRequired;
            if (!record.document_revisions) return error.InvalidTxnRecord;
            {
                var scan = try self.store.beginCurrentScan();
                defer scan.abort();
                var members = try document_revision_reservation.prepareCleanup(revision_alloc, &scan, held);
                defer members.deinit();
                const prefix = document_revision_reservation.rowPrefix(txn_id);
                var locks = try self.store.beginProbe();
                defer locks.abort();
                for (members.keys[1..]) |row_key| {
                    const lock_key = try std.mem.concat(revision_alloc, u8, &.{ intent_locks_prefix, row_key[prefix.len..] });
                    const lock_owner = try locks.get(lock_key);
                    if (lock_owner.len != 16 or !std.mem.eql(u8, lock_owner, &txn_id)) return error.InvalidTxnRecord;
                    try deletes.append(self.alloc, lock_key);
                }
            }
            var read = try self.store.beginProbe();
            defer read.abort();
            // ABORT's revision-fenced fast path carries keys only; it never
            // needs payloads or creates committed row receipts.
            if (status == .committed) for (intent_entries) |entry| {
                const key = entry.key[intents_prefix.len + 17 ..];
                if (isRawMetadataIntentKey(key)) continue;
                const assigned = (try IntentValue.decode(entry.value)).document_revision orelse return error.InvalidTxnRecord;
                const receipt_key = try document_revision_reservation.rowKey(revision_alloc, txn_id, key);
                const receipt = try read.get(receipt_key);
                if (!try document_revision_reservation.rowUsed(receipt) or try document_revision_reservation.rowVersion(receipt) != assigned) return error.InvalidTxnRecord;
            };
            if (status == .committed) try self.validateReservedMaterialization(revision_alloc, intent_entries, extra_batch);
        }
        if (reservation == null and status == .committed and record.document_revisions and !extra_batch.skip_all_intent_application) {
            var revision_writes = std.ArrayList(docstore.KVPair).empty;
            var revision_deletes = std.ArrayList([]const u8).empty;
            for (intent_entries) |entry| {
                const key = entry.key[intents_prefix.len + 17 ..];
                if (isRawMetadataIntentKey(key) or skipped_intent_keys.contains(key)) continue;
                const intent = try IntentValue.decode(entry.value);
                if (intent.value) |value| try revision_writes.append(revision_alloc, .{ .key = key, .value = value }) else try revision_deletes.append(revision_alloc, key);
            }
            if (try ttl.nextDocumentVersion(self.store, revision_alloc, timestamp, revision_writes.items, revision_deletes.items)) |version| {
                resolved_version = version;
                const bytes = try revision_alloc.alloc(u8, 8);
                std.mem.writeInt(u64, bytes[0..8], version, .little);
                try writes.append(self.alloc, .{ .key = ttl.document_version_watermark_key, .value = bytes });
            }
        }
        if (status == .committed) {
            // Apply intents to real keys
            for (intent_entries) |entry| {
                // Extract user key from intent key:
                // intents_prefix(20) + txn_id(16) + ':'(1) + user_key
                const user_key = entry.key[intents_prefix.len + 17 ..];
                if (extra_batch.skip_all_intent_application and extra_batch.prepared_graph_effects_checked) continue;
                const intent = try IntentValue.decode(entry.value);
                if ((intent.document_revision != null) != (reservation != null and !isRawMetadataIntentKey(user_key))) return error.InvalidTxnRecord;
                if (intent.prepared_graph_effects != null and !extra_batch.prepared_graph_effects_checked) return error.PreparedIntentRequiresMaterialization;
                if (extra_batch.skip_all_intent_application or skipped_intent_keys.contains(user_key)) continue;
                // The checked graph attachment has already been materialized
                // into this same atomic extra batch. Its literal JSON primary
                // value still follows the ordinary document-intent path.
                // Prepared native rows require their separate primary mapper.
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
                    try owned_apply_keys.append(self.alloc, store_key);
                    try deletes.append(self.alloc, store_key);

                    const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, user_key);
                    try owned_apply_keys.append(self.alloc, ts_key);
                    try deletes.append(self.alloc, ts_key);
                } else {
                    // Put
                    const val = intent.value.?;
                    const store_key = try internal_keys.documentKeyAlloc(self.alloc, user_key);
                    try owned_apply_keys.append(self.alloc, store_key);
                    try writes.append(self.alloc, .{ .key = store_key, .value = val });

                    // Write timestamp for the key
                    const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, user_key);
                    try owned_apply_keys.append(self.alloc, ts_key);
                    const ts_val = try ttl.encodeTimestampAlloc(revision_alloc, intent.document_revision orelse resolved_version, @as(i128, timestamp));
                    try writes.append(self.alloc, .{ .key = ts_key, .value = ts_val });
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
        if (!retain_metadata_ack) try indexed_ack.append(self.alloc, &writes);
        if (!retain_metadata_ack) if (resolved_participant_value) |value| {
            try writes.append(self.alloc, .{ .key = &resolved_participant_key, .value = value });
        };
        try deletes.appendSlice(self.alloc, extra_batch.deletes);
        try deletes.appendSlice(self.alloc, extra_batch.completion_deletes);

        if (status == .committed) {
            if (try self.getCompletionAdmission(self.alloc, txn_id)) |saved| {
                var plan = saved;
                defer plan.deinit();
                const request: completion.ResolutionRequest = .{ .transaction_id = txn_id, .admission_digest = plan.value.digest, .commit_version = record.commit_version, .record_digest = completion.bytesDigest(rec_val) };
                completion_request = try request.encode();
                try writes.append(self.alloc, .{ .key = &completion_request_key, .value = &completion_request });
            }
        }

        const admission = try self.loadIntentAdmission(self.alloc, txn_id);
        const ticket = try self.applyBatchStagedWithHistory(writes.items, deletes.items, extra_batch.replay, .{
            .previous = if (admission) |value| value.reservation() else 0,
            .next = 0,
            .terminal = if (was_terminal) null else publication_terminal_sink,
            .source_resolution = if (was_terminal or extra_batch.replication_resolution == null) null else .{
                .input = extra_batch.replication_resolution.?,
                .revision = record.intent_revision,
                .status = status,
            },
        }, if (status == .committed) extra_batch.commit_participant else null, extra_batch.source_scope_cut, extra_batch.source_retirement, legacy_terminal);

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
        return .{ .outcome = .{
            .applied = true,
            .replay_sequence = record.replay_sequence,
        }, .ticket = ticket };
    }

    pub fn collectIntentBatch(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !IntentBatch {
        // Read the revision before scanning. Intent writes publish the record
        // revision and intent rows in one backend batch, so validation under
        // the DB apply lock detects any prepare that raced with this snapshot.
        const record = try self.loadTransactionRecord(txn_id);
        const schema_binding = try self.loadSchemaBinding(alloc, txn_id);
        const prepared_key = makeSidecarKey(prepared_enrichments_prefix, txn_id);
        const prepared_json = self.getAlloc(alloc, &prepared_key) catch |err| if (err == error.NotFound) null else return err;
        errdefer if (prepared_json) |bytes| alloc.free(bytes);
        var intent_prefix_buf: [intents_prefix.len + 17]u8 = undefined;
        @memcpy(intent_prefix_buf[0..intents_prefix.len], intents_prefix);
        @memcpy(intent_prefix_buf[intents_prefix.len..][0..16], &txn_id);
        intent_prefix_buf[intents_prefix.len + 16] = ':';
        const scan_prefix = intent_prefix_buf[0 .. intents_prefix.len + 17];

        const intent_entries = try self.loadIntentEntries(alloc, txn_id, scan_prefix);
        errdefer backend_scan.freeResults(alloc, intent_entries);

        var has_graph_effects = false;
        var write_count: usize = 0;
        for (intent_entries) |entry| {
            const value = try IntentValue.decode(entry.value);
            if (value.value != null) write_count += 1;
            has_graph_effects = has_graph_effects or value.prepared_graph_effects != null;
        }
        const writes = try alloc.alloc(docstore.KVPair, write_count);
        errdefer alloc.free(writes);
        const prepared_rows = try alloc.alloc(?[]const u8, write_count);
        errdefer alloc.free(prepared_rows);
        const graph_effects: []?[]const u8 = if (has_graph_effects) try alloc.alloc(?[]const u8, write_count) else &.{};
        errdefer if (graph_effects.len != 0) alloc.free(graph_effects);
        const deletes = try alloc.alloc([]const u8, intent_entries.len - write_count);
        errdefer alloc.free(deletes);
        var references: std.ArrayList(document_revision_reservation.Reference) = .empty;
        errdefer references.deinit(alloc);
        var writes_initialized: usize = 0;
        var deletes_initialized: usize = 0;

        for (intent_entries) |entry| {
            const user_key = entry.key[intents_prefix.len + 17 ..];
            const intent = try IntentValue.decode(entry.value);
            if (intent.document_revision) |version| try references.append(alloc, .{ .key = user_key, .version = version });
            if (intent.value == null) {
                deletes[deletes_initialized] = user_key;
                deletes_initialized += 1;
            } else {
                writes[writes_initialized] = .{ .key = user_key, .value = intent.value.? };
                prepared_rows[writes_initialized] = intent.prepared_row;
                if (has_graph_effects) graph_effects[writes_initialized] = intent.prepared_graph_effects;
                writes_initialized += 1;
            }
        }
        return .{ .ordinary_producer = record.ordinary_producer, .legacy_producer = record.legacy_producer, .document_revision_references = try references.toOwnedSlice(alloc), .prepared_enrichments_json = prepared_json, .writes = writes, .deletes = deletes, .prepared_rows = prepared_rows, .prepared_graph_effects = graph_effects, .revision = record.intent_revision, .schema_binding = schema_binding, .owned_entries = intent_entries, .document_revisions = record.document_revisions };
    }

    pub fn loadPreparedEnrichments(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !?[]u8 {
        const selected = makeSidecarKey(prepared_enrichments_prefix, txn_id);
        return self.getAlloc(alloc, &selected) catch |err| if (err == error.NotFound) null else err;
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
        if (try cursor.seekAtOrAfter(schema_leases_prefix)) |entry|
            if (std.mem.startsWith(u8, entry.key, schema_leases_prefix)) return true;
        // Existing document databases may have unresolved intents predating
        // relational epoch binding. They also fence a storage-mode switch.
        const entry = (try cursor.seekAtOrAfter(intents_prefix)) orelse return false;
        return std.mem.startsWith(u8, entry.key, intents_prefix);
    }

    /// Private migration work discovery. Each page uses one read snapshot,
    /// including the immutable physical closure. Pagination is not a coherent
    /// all-record cut and an empty page is never a drainage certificate.
    pub const LegacyRecoveryItem = @import("source_legacy_inventory.zig").Item;
    pub const LegacyRecoveryPage = @import("source_legacy_inventory.zig").Page;
    pub fn legacyRecoveryPage(self: *TxnManager, alloc: Allocator, root: source_scope.Basis, after: ?TxnId, max_records: usize) !LegacyRecoveryPage {
        if (max_records == 0 or max_records > 256) return error.InvalidSourceAdmission;
        var read = try self.store.beginRead();
        defer read.abort();
        return legacyRecoveryPageInRead(alloc, &read, root, after, max_records);
    }

    fn legacyRecordNeedsRecovery(record: TxnRecord) bool {
        return !record.ordinary_producer and record.source_scope == null and
            (record.status == .pending or !record.intents_resolved_known or !record.intents_resolved);
    }

    /// First binding must check the entire record keyspace in the same native
    /// writer that installs strict admission. Remote inventories cannot
    /// certify this cut. The persisted closure fences new legacy records.
    pub fn requireLegacyDrainedInWriter(alloc: Allocator, writer: anytype, root: source_scope.Basis) !void {
        _ = (try @import("source_producer_policy.zig").Store.read(alloc, writer, root)) orelse return error.SourcePendingWork;
        var cursor = try writer.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(records_prefix);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, records_prefix)) break;
            if (kv.key.len != records_prefix.len + 16 or kv.value.len > 4096) return error.InvalidTxnRecord;
            if (legacyRecordNeedsRecovery(try decodeRecord(kv.value))) return error.SourcePendingWork;
        }
    }

    /// Reuse the caller's read snapshot so physical authority, applied marker,
    /// closure and inspected records cannot come from different cuts.
    pub fn legacyRecoveryPageInRead(alloc: Allocator, read: anytype, root: source_scope.Basis, after: ?TxnId, max_records: usize) !LegacyRecoveryPage {
        if (max_records == 0 or max_records > @import("source_legacy_inventory.zig").max_records) return error.InvalidSourceAdmission;
        const closure = (try @import("source_producer_policy.zig").Store.read(alloc, read, root)) orelse return error.SourcePendingWork;
        var cursor = try read.openCursor();
        defer cursor.close();
        var items: std.ArrayListUnmanaged(LegacyRecoveryItem) = .empty;
        errdefer items.deinit(alloc);
        var scanned: usize = 0;
        var bytes: usize = 0;
        var last = after;
        const start = if (after) |id| makeRecordKey(id) else makeRecordKey(@splat(0));
        var entry = try cursor.seekAtOrAfter(if (after != null) &start else records_prefix);
        if (after != null) if (entry) |kv| {
            if (std.mem.eql(u8, kv.key, &start)) entry = try cursor.next();
        };
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, records_prefix)) break;
            if (kv.key.len != records_prefix.len + 16) return error.InvalidTxnRecord;
            // Bound inspected bytes as well as returned items. Oversized or
            // corrupt records fail closed rather than being skipped forever.
            if (kv.value.len > 4096) return error.InvalidTxnRecord;
            if (scanned == max_records or bytes + kv.key.len + kv.value.len > 1024 * 1024) {
                return .{ .closure = closure, .items = try items.toOwnedSlice(alloc), .scanned_records = scanned, .next_after = last, .reached_end = false };
            }
            const record = try decodeRecord(kv.value);
            scanned += 1;
            bytes += kv.key.len + kv.value.len;
            const id: TxnId = kv.key[records_prefix.len..][0..16].*;
            last = id;
            if (!legacyRecordNeedsRecovery(record)) continue;
            try items.append(alloc, .{ .transaction_id = id, .record_digest = completion.bytesDigest(kv.value), .status = record.status, .prepared = record.prepared, .prepared_known = record.prepared_known, .coordinator = record.coordinator, .coordinator_known = record.coordinator_known, .intents_resolved = record.intents_resolved, .intents_resolved_known = record.intents_resolved_known });
        }
        return .{ .closure = closure, .items = try items.toOwnedSlice(alloc), .scanned_records = scanned, .next_after = last, .reached_end = true };
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
                try self.applyBatch(&.{}, &.{&key}, null);
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
            if (intent.prepared_row != null or intent.prepared_graph_effects != null) return error.PreparedIntentRequiresMaterialization;
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
        return record.coordinator and (record.retain_terminal or try self.keyExists(&makeSidecarKey(completion_admission_prefix, txn_id)));
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
        return hasTopologySensitiveTransactionsInView(self.alloc, &read);
    }

    /// Reuse the caller's pinned snapshot or writer view. Schema publication
    /// must not validate its directory against a second, differently cut read.
    pub fn hasTopologySensitiveTransactionsInView(alloc: Allocator, read: anytype) !bool {
        return hasTopologySensitiveTransactionsPolicy(alloc, read, false);
    }

    /// A clean process epoch cannot strand even terminal external ACK debt.
    /// Ordinary topology work may move that bookkeeping; native restart
    /// changes its execution epoch and therefore must wait for exact ACKs.
    pub fn hasRelationLifetimeDebtInView(alloc: Allocator, read: anytype) !bool {
        return hasTopologySensitiveTransactionsPolicy(alloc, read, true);
    }

    /// Preserve PostgreSQL UNLOGGED prepared transactions on a clean restart.
    /// Completed ordinary journals may remain; every persisted effect must
    /// belong to an actual root-pinned external
    /// decision. This streams keys without materializing intents or history;
    /// orphan effects and unrelated pessimistic owners remain fail-closed.
    pub fn requireCleanNativeRecoveryInView(alloc: Allocator, read: anytype, pin: NativePhysicalPin) !void {
        try pin.validate();
        if (try readHasPrefix(read, hot_standby_batch_outbox_prefix) or try readHasPrefix(read, hot_standby_replay_outbox_prefix))
            return error.SqlRelationRecoveryIncomplete;
        if (try @import("pessimistic_lock/resource.zig").Binding.load(read)) |binding| {
            if (try @import("pessimistic_lock/authority.zig").activeCount(read, binding.namespace) != 0) return error.SqlRelationRecoveryIncomplete;
        }
        var cursor = try read.openCursor();
        defer cursor.close();
        inline for (.{ records_prefix, intents_prefix, intent_locks_prefix, read_guards_prefix, read_members_prefix, schema_leases_prefix }) |prefix| {
            var next = try cursor.seekAtOrAfter(prefix);
            while (next) |entry| {
                if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                if (comptime std.mem.eql(u8, prefix, records_prefix)) {
                    if (entry.key.len != prefix.len + 16) return error.InvalidTxnRecord;
                }
                if (comptime std.mem.eql(u8, prefix, intents_prefix)) {
                    if (entry.key.len < prefix.len + 17 or entry.key[prefix.len + 16] != ':') return error.InvalidTxnRecord;
                }
                const id: TxnId = if (comptime std.mem.eql(u8, prefix, intent_locks_prefix)) blk: {
                    if (entry.value.len != 16) return error.InvalidTxnRecord;
                    break :blk entry.value[0..16].*;
                } else if (comptime std.mem.eql(u8, prefix, read_guards_prefix)) blk: {
                    if (entry.key.len < prefix.len + 4 + 16) return error.InvalidTxnRecord;
                    const length = std.mem.readInt(u32, entry.key[prefix.len..][0..4], .big);
                    if (entry.key.len - prefix.len - 4 - 16 != length) return error.InvalidTxnRecord;
                    break :blk entry.key[entry.key.len - 16 ..][0..16].*;
                } else blk: {
                    if (entry.key.len < prefix.len + 16) return error.InvalidTxnRecord;
                    break :blk entry.key[prefix.len..][0..16].*;
                };
                const key = makeSidecarKey(records_prefix, id);
                const bytes = read.get(&key) catch |err| switch (err) {
                    error.NotFound => return error.SqlRelationRecoveryIncomplete,
                    else => return err,
                };
                const record = try decodeRecord(bytes);
                if (comptime !std.mem.eql(u8, prefix, records_prefix)) {
                    if (record.intents_resolved_known and record.intents_resolved) return error.InvalidTxnRecord;
                }
                if (comptime std.mem.eql(u8, prefix, records_prefix)) {
                    // Ordinary validation coordinators leave terminal journal
                    // history. It is not an external prepared vote or a live
                    // effect; the remaining prefix walks still reject every
                    // orphan or resolved record with leftover effects.
                    if (record.external_decision_owner == null and record.native_pin == null and
                        record.status != .pending and record.intents_resolved_known and record.intents_resolved and
                        !try hasUnresolvedParticipantsInRead(alloc, read, id))
                    {
                        next = try cursor.next();
                        continue;
                    }
                }
                const actual = record.native_pin orelse return error.SqlRelationRecoveryIncomplete;
                const owner = record.external_decision_owner orelse return error.SqlRelationRecoveryIncomplete;
                try owner.validate();
                if (!actual.eql(pin)) return error.SqlRelationRecoveryIncomplete;
                next = try cursor.next();
            }
        }
    }

    fn hasTopologySensitiveTransactionsPolicy(alloc: Allocator, read: anytype, include_external_ack: bool) !bool {
        // Enrollment owns identity before the first row grant. Moving or
        // restoring that owner must not strand even an empty, fenced scope.
        if (try @import("pessimistic_lock/resource.zig").Binding.load(read)) |binding| {
            if (try @import("pessimistic_lock/authority.zig").activeCount(read, binding.namespace) != 0) return true;
        }

        if (try @import("source_plan_storage.zig").hasRetainedArtifacts(read)) return true;
        return hasRowTransactionStateInView(alloc, read, include_external_ack);
    }

    /// Range tracking adds row-version barriers; pure pessimistic owners do
    /// not carry an older range proof and can safely cross this activation.
    /// Keep pending native votes/intents/outboxes protected exactly as before.
    pub fn hasRangeActivationTransactions(self: *TxnManager) !bool {
        var read = try self.store.beginRead();
        defer read.abort();
        return hasRowTransactionStateInView(self.alloc, &read, false);
    }

    pub fn hasPublicationCaptureDebtInView(alloc: Allocator, read: anytype) !bool {
        if (try readHasPrefix(read, schema_leases_prefix)) return true;
        return hasRowTransactionStateInView(alloc, read, false);
    }

    fn hasRowTransactionStateInView(alloc: Allocator, read: anytype, include_external_ack: bool) !bool {
        // Resolution normally removes intents and HA outboxes atomically before
        // a transaction becomes quiescent. Check the global prefixes once rather
        // than opening an intent cursor for every retained terminal record.
        if (try readHasPrefix(read, intents_prefix) or
            try readHasPrefix(read, intent_locks_prefix) or
            try readHasPrefix(read, read_guards_prefix) or
            try readHasPrefix(read, read_members_prefix) or
            try readHasPrefix(read, hot_standby_batch_outbox_prefix) or
            try readHasPrefix(read, hot_standby_replay_outbox_prefix))
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
                // Metadata owns external cohort fan-out and its catalog pins.
                // A terminal native participant with all local intents drained
                // carries retention/ACK bookkeeping, not outstanding row work.
                // Its copied roster is not a native coordinator resolution set.
                if (!include_external_ack and record.external_decision_owner != null and record.intents_resolved_known and record.intents_resolved) {
                    entry = try cursor.next();
                    continue;
                }

                const txn_id = record_entry.key[records_prefix.len..][0..16].*;
                if (try hasUnresolvedParticipantsInRead(alloc, read, txn_id)) return true;
            }
            entry = try cursor.next();
        }
        return false;
    }

    // Shared by lifetime-debt and native close checks. Retained terminal
    // history is quiescent only after the actual enlisted roster is resolved.
    fn hasUnresolvedParticipantsInRead(alloc: Allocator, read: anytype, txn_id: TxnId) !bool {
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
                const members = try resolvedParticipantsInRead(alloc, read, txn_id);
                defer freeParticipantList(alloc, members);
                if (members.len > indexed.enlisted) return error.InvalidTxnRecord;
                if (members.len != indexed.enlisted) return true;
            } else if (indexed.resolved != indexed.enlisted) return true;
            return false;
        } else |err| if (err != error.NotFound) return err;
        const participant_count = try participantListCountInRead(read, &participant_key);
        const resolved_count = try participantListCountInRead(read, &resolved_key);
        if (resolved_count > participant_count) return TxnError.InvalidTxnRecord;
        // markParticipantResolved only appends unique, enlisted members,
        // so equal validated counts prove that the participant set is
        // fully acknowledged without allocating or comparing strings.
        if (resolved_count != participant_count) return true;
        return false;
    }

    /// Bounded uncertainty check under primary AND replay capture admission.
    /// Ordinary intents may resolve without an all-owner activation barrier,
    /// so they still block. External PREPARED votes expose only old primary
    /// rows; a durable activation counter fences their eventual publication.
    pub fn hasStatementReadDebt(self: *TxnManager) !bool {
        var read = try self.store.beginRead();
        defer read.abort();
        return statementReadHasDebt(&read);
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
                .ordinary_producer = record.ordinary_producer,
                .legacy_producer = record.legacy_producer,
                .source_purpose = record.source_purpose,
                .source_scope = record.source_scope,
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
                .external_decision_owner = record.external_decision_owner,
                .native_pin = record.native_pin,
                .external_commit_started = record.external_commit_started,
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
        try self.requireNativeMutationPin(extra_batch.native_pin);
        const index_key = makeSidecarKey(participant_index_prefix, txn_id);
        if (try self.keyExists(&index_key)) return self.markParticipantsResolvedExtraBatch(txn_id, &.{participant}, extra_batch);
        // A replicated acknowledgement can be retried after the coordinator
        // has already cleaned the transaction. Do not recreate an orphaned
        // resolved-participants sidecar in that case, and reject corrupt
        // acknowledgements for participants that were never enlisted.
        if ((try self.loadTransactionRecord(txn_id)).replication_source_required) return error.ReplicationCutoverAuthorityLost;
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
        try self.requireNativeMutationPin(extra_batch.native_pin);
        if (acknowledgements.len == 0 or acknowledgements.len > 64) return error.InvalidParticipant;
        const record = try self.loadTransactionRecord(txn_id);
        if (record.replication_source_required != (extra_batch.replication_ack != null)) return error.ReplicationCutoverAuthorityLost;
        if (extra_batch.replication_ack) |ack| {
            const source_ack = ack.completion;
            const owner = record.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
            if (!source_ack.permit.decision_owner.eql(owner) or !std.mem.eql(u8, &source_ack.permit.reference.txn_id, &txn_id) or
                record.status == .pending or !record.intents_resolved or ack.position != .raft or
                (source_ack.decision == .committed) != (record.status == .committed) or
                (record.status == .committed and record.commit_version != source_ack.decision_version))
                return error.ReplicationCutoverAuthorityLost;
            try source_ack.requireAcknowledgementIds(acknowledgements);
        }
        if (extra_batch.external_decision_owner) |expected| {
            const held = record.external_decision_owner orelse return error.ExternalTransactionDecisionChanged;
            if (!held.eql(expected)) return error.ExternalTransactionDecisionChanged;
            if (record.status == .pending) return error.ExternalTransactionDecisionRequired;
        }
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var recovered_roster = false;
        if (extra_batch.replication_ack) |ack| {
            const roster = try self.getParticipants(scratch, txn_id);
            recovered_roster = roster.len == 0 and record.status == .aborted and !record.prepared;
            try ack.completion.requireParticipants(if (recovered_roster) acknowledgements else roster);
        }
        const index_key = makeSidecarKey(participant_index_prefix, txn_id);
        const raw = self.getAlloc(scratch, &index_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var indexed = if (raw) |bytes| try ParticipantIndex.decode(bytes) else ParticipantIndex{};
        var writes: std.ArrayListUnmanaged(docstore.KVPair) = .empty;
        var deletes: std.ArrayListUnmanaged([]const u8) = .empty;
        var enlisted: std.StringHashMapUnmanaged(void) = .empty;
        if (recovered_roster) {
            if (raw != null) return error.InvalidTxnRecord;
            const members_key = makeSidecarKey(participants_prefix, txn_id);
            try writes.append(scratch, .{ .key = try scratch.dupe(u8, &members_key), .value = try encodeParticipantList(scratch, acknowledgements) });
        }
        if (raw == null) {
            const participants = if (recovered_roster) acknowledgements else try self.getParticipants(scratch, txn_id);
            for (participants) |participant| {
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
            if (raw == null) {
                if (!enlisted.contains(participant)) return error.InvalidParticipant;
            } else {
                const key = makeParticipantIndexKey(participant_index_prefix, txn_id, participant);
                const member = self.getAlloc(scratch, &key) catch |err| switch (err) {
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
        const legacy_members = try self.loadParticipantSet(scratch, resolved_participants_prefix, txn_id);
        for (legacy_members) |participant| {
            if (raw == null) {
                if (!enlisted.contains(participant)) return error.InvalidTxnRecord;
            } else {
                const member_key = makeParticipantIndexKey(participant_index_prefix, txn_id, participant);
                const member = self.getAlloc(scratch, &member_key) catch |err| switch (err) {
                    error.NotFound => return error.InvalidTxnRecord,
                    else => return err,
                };
                if (!std.mem.eql(u8, member, participant)) return error.InvalidTxnRecord;
            }
            try resolved.put(scratch, participant, {});
        }
        for (acknowledgements) |participant| try resolved.put(scratch, participant, {});
        var members = resolved.keyIterator();
        while (members.next()) |participant| {
            const key = makeParticipantIndexKey(resolved_index_prefix, txn_id, participant.*);
            const existing = self.getAlloc(scratch, &key) catch |err| switch (err) {
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
        try writes.append(scratch, .{ .key = &index_key, .value = &encoded });
        try writes.appendSlice(scratch, extra_batch.writes);
        try deletes.append(scratch, &legacy_key);
        try deletes.appendSlice(scratch, extra_batch.deletes);
        // Membership migration, resolution entries, count proof and the Raft
        // replay marker publish in one storage batch. No growing list rewrite.
        if (extra_batch.replication_ack) |ack| {
            const ticket = try self.applyBatchStaged(writes.items, deletes.items, null, .{ .previous = 0, .next = 0, .source_ack = ack }, null);
            try ticket.wait();
        } else try self.applyBatch(writes.items, deletes.items, null);
    }

    /// Caller holds its DB apply fence across record and roster observation.
    pub fn externalRetentionSnapshot(self: *TxnManager, a: Allocator, id: TxnId, owner: ExternalDecisionOwner, pin: NativePhysicalPin) !ExternalRetentionSnapshot {
        try owner.validate();
        const record = try self.loadTransactionRecord(id);
        const held = record.external_decision_owner orelse return error.ExternalTransactionDecisionChanged;
        if (!held.eql(owner) or !std.meta.eql(record.native_pin, @as(?NativePhysicalPin, pin))) return error.ExternalTransactionDecisionChanged;
        if (!record.coordinator_known or !record.prepared_known or !record.intents_resolved_known) return error.InvalidTxnRecord;
        const participants = try self.getParticipants(a, id);
        errdefer freeParticipantList(a, participants);
        if (participants.len > 64) return error.InvalidTxnRecord;
        return .{ .status = record.status, .coordinator = record.coordinator, .prepared = record.prepared, .intents_resolved = record.intents_resolved, .participants = participants };
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
                if (summary.external_decision_owner) |owner| {
                    try owner.validate();
                    if (!summary.coordinator_known) return error.InvalidTxnRecord;
                    // The metadata record alone can authorize abort.
                    stats.kept_recent_pending += 1;
                    continue;
                }
                if (summary.source_purpose != null) {
                    // Source-control recovery owns this original reservation;
                    // elapsed time cannot invalidate an acknowledged source vote.
                    stats.kept_recent_pending += 1;
                    continue;
                }
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
            if (!try self.nativeLockCleanupReady(txn_id)) {
                stats.deferred_unresolved += 1;
                continue;
            }
            const no_intents = if (refreshed.intents_resolved_known and refreshed.intents_resolved)
                true
            else
                !try self.hasAnyIntents(txn_id);
            if (refreshed.status != .pending and terminalRetentionElapsed(refreshed, cutoff_timestamp, options.retained_cutoff_timestamp orelse cutoff_timestamp) and
                no_intents and !try self.hasReplicationOutbox(txn_id))
            {
                try self.deleteTransactionMetadataKnownRecord(txn_id, refreshed, .{});
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
        var relational_primary: ?bool = null;
        for (predicates) |pred| {
            if (pred.expected_mutation_revision) |revision| {
                if (revision == 0 or pred.comparison != .document_version or pred.expected_version == 0 or pred.expected_content_digest == null)
                    return error.InvalidArgument;
            }
            if (pred.unique_absence and (pred.comparison != .document_version or pred.expected_version != 0)) return error.InvalidArgument;
            switch (pred.comparison) {
                .document_version => {
                    const current_ts = try self.readTimestamp(pred.key);
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
                            const catalog_bytes = self.getAlloc(self.alloc, table_catalog.key) catch |err| switch (err) {
                                error.NotFound => null,
                                else => return err,
                            };
                            defer if (catalog_bytes) |bytes| self.alloc.free(bytes);
                            if (catalog_bytes) |bytes| {
                                const catalog = try table_catalog.Catalog.decode(bytes);
                                relational_primary = catalog.mode_initialized and catalog.storage_mode == .relational;
                            } else {
                                // Legacy document stores predate the fixed catalog.
                                const schema = try @import("schema.zig").loadSchema(self.store, self.alloc);
                                defer if (schema) |value| @import("schema.zig").freeSchema(self.alloc, value);
                                relational_primary = schema != null and schema.?.storage_mode == .relational;
                            }
                        }
                        const primary_key = if (relational_primary.?)
                            try internal_keys.relationalRowKeyAlloc(self.alloc, pred.key)
                        else
                            try internal_keys.documentKeyAlloc(self.alloc, pred.key);
                        defer self.alloc.free(primary_key);
                        var probe = try self.store.beginProbe();
                        defer probe.abort();
                        if (pred.expected_mutation_revision) |revision| {
                            const revisions = @import("document_mutation_revision.zig");
                            const revision_key = revisions.keyForPhysical(primary_key) orelse return error.InvalidArgument;
                            if (try revisions.load(&probe, revision_key) != revision) return TxnError.VersionConflict;
                        }
                        const current = probe.get(primary_key) catch |err| switch (err) {
                            error.NotFound => return TxnError.VersionConflict,
                            else => return err,
                        };
                        var digest: [32]u8 = undefined;
                        std.crypto.hash.sha2.Sha256.hash(current, &digest, .{});
                        if (!std.mem.eql(u8, &digest, &expected)) return TxnError.VersionConflict;
                    }
                },
                .exact_value => {
                    if (!isRawMetadataPredicateKey(pred.key)) return error.InvalidArgument;
                    if (range_protection.counterBucket(pred.key)) |id| {
                        var probe = try self.store.beginProbe();
                        defer probe.abort();
                        if (!try range_protection.isActive(&probe)) return error.SqlRangeTrackingRequired;
                        var read = try self.store.beginCurrentScan();
                        defer read.abort();
                        var cursor = try read.openCursor();
                        defer cursor.close();
                        const writer_key = range_protection.writerKey(id);
                        try self.checkReadGuardCursor(&cursor, &writer_key, exclude_txn);
                    }
                    if (range_protection.indexCounterDigest(pred.key)) |digest| {
                        var probe = try self.store.beginProbe();
                        defer probe.abort();
                        if (!try range_protection.isActive(&probe)) return error.SqlRangeTrackingRequired;
                        var read = try self.store.beginCurrentScan();
                        defer read.abort();
                        var cursor = try read.openCursor();
                        defer cursor.close();
                        const writer_key = range_protection.indexWriterKey(digest);
                        try self.checkReadGuardCursor(&cursor, &writer_key, exclude_txn);
                    }
                    const current = self.getAlloc(self.alloc, pred.key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    defer if (current) |bytes| self.alloc.free(bytes);
                    if (pred.expected_value) |expected| {
                        if (!std.mem.eql(u8, current orelse return TxnError.VersionConflict, expected)) return TxnError.VersionConflict;
                    } else if (current != null) return TxnError.VersionConflict;
                },
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
        if (try self.readGuardCount() != 0) {
            var scan = try self.store.beginCurrentScan();
            defer scan.abort();
            var cursor = try scan.openCursor();
            defer cursor.close();
            var buckets: std.StaticBitSet(range_protection.bucket_count) = .empty;
            for (intents) |intent| {
                try self.checkReadGuardCursor(&cursor, intent.key, exclude_txn);
                if (std.mem.startsWith(u8, intent.key, "\x00\x00")) continue;
                const id = range_protection.bucket(intent.key);
                if (buckets.isSet(id)) continue;
                buckets.set(id);
                const counter = range_protection.counterKey(id);
                try self.checkReadGuardCursor(&cursor, &counter, exclude_txn);
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
        try self.checkIntentConflicts(&.{.{ .key = key, .value = null }}, null);
    }

    /// Check a complete ordinary write batch with one sorted point probe.
    /// Intent locks are independent keys, so opening a read transaction per
    /// user key only multiplies snapshot/layout work without strengthening the
    /// conflict guarantee. The DB apply mutex keeps this batch check
    /// linearizable with concurrent intent admission.
    pub fn checkOrdinaryWriteConflicts(self: *TxnManager, user_keys: []const []const u8) !void {
        if (user_keys.len == 0) return;
        var probe = try self.store.beginProbe();
        defer probe.abort();
        try self.checkOrdinaryWriteConflictsWithProbe(user_keys, &probe);
    }

    /// Same as `checkOrdinaryWriteConflicts`, reading through an already-open
    /// probe/read transaction instead of opening (and aborting) its own.
    /// perf/lwa-write-overhead: lets a mutation's own prep phase share one
    /// transaction across this and its other read-only checks instead of
    /// paying a separate overlay pin/unpin for each. `probe` need only
    /// support `getManySorted`/`get`, matching both `backend_erased.ProbeTxn`
    /// (this function's own default) and `docstore.DocStore.Txn` (the
    /// concrete type the DB layer's shared prep-phase probe uses).
    pub fn checkOrdinaryWriteConflictsWithProbe(self: *TxnManager, user_keys: []const []const u8, probe: anytype) !void {
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
        if (digests.len == 0 or try self.readGuardCount() == 0) return;
        var scan = try self.store.beginCurrentScan();
        defer scan.abort();
        var cursor = try scan.openCursor();
        defer cursor.close();
        for (digests) |digest| {
            const counter = range_protection.indexCounterKey(digest);
            try self.checkReadGuardCursor(&cursor, &counter, exclude_txn);
        }
    }

    fn readGuardCount(self: *TxnManager) !u64 {
        const raw = self.getAlloc(self.alloc, read_guard_count_key) catch |err| switch (err) {
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
        const key = makeSidecarKey(read_admission_prefix, txn_id);
        const raw = self.getAlloc(self.alloc, &key) catch |err| switch (err) {
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
        const length = std.math.cast(u32, user_key.len) orelse return error.TransactionTooLarge;
        const prefix_length = read_guards_prefix.len + 4 + user_key.len;
        const key = try alloc.alloc(u8, prefix_length + @as(usize, if (txn_id != null) 16 else 0));
        @memcpy(key[0..read_guards_prefix.len], read_guards_prefix);
        std.mem.writeInt(u32, key[read_guards_prefix.len..][0..4], length, .big);
        @memcpy(key[read_guards_prefix.len + 4 ..][0..user_key.len], user_key);
        if (txn_id) |id| @memcpy(key[prefix_length..], &id);
        return key;
    }

    /// At most two entries: our own shared guard and the first other reader.
    /// Exact length framing prevents a guard for "a" locking "a\x00" or "ab".
    fn checkReadGuardCursor(self: *TxnManager, cursor: anytype, key: []const u8, exclude_txn: ?TxnId) !void {
        const prefix = try makeReadGuardKey(self.alloc, key, null);
        defer self.alloc.free(prefix);
        var entry = try cursor.seekAtOrAfter(prefix);
        while (entry) |item| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, item.key, prefix)) break;
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
    ) !ReadAdmissionChange {
        if (predicates.len > max_read_guards_per_transaction) return error.TransactionTooLarge;
        const previous = try self.loadReadAdmission(txn_id);
        var next = previous;
        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(self.alloc);
        const member_prefix = makeSidecarKey(read_members_prefix, txn_id);
        for (predicates) |predicate| {
            if (write_set.contains(predicate.key) or (try seen.getOrPut(self.alloc, predicate.key)).found_existing) continue;
            const member = try std.mem.concat(self.alloc, u8, &.{ &member_prefix, predicate.key });
            try appendOwnedBytes(self.alloc, owned_keys, member);
            const old = self.getAlloc(self.alloc, member) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (old) |bytes| self.alloc.free(bytes);
            var identity_buf: [33]u8 = undefined;
            const identity = readPredicateIdentity(predicate, &identity_buf);
            if (old) |bytes| {
                if (!validReadMember(bytes)) return error.InvalidTxnRecord;
                if (!std.mem.eql(u8, bytes, identity)) return error.VersionConflict;
                continue;
            }
            next.count = std.math.add(u64, next.count, 1) catch return error.TransactionTooLarge;
            if (next.count > max_read_guards_per_transaction) return error.TransactionTooLarge;
            next.bytes = std.math.add(u64, next.bytes, try intentAdmissionBytes(.{ .key = predicate.key, .value = null })) catch return error.TransactionTooLarge;
            const version = try self.alloc.dupe(u8, identity);
            try appendOwnedBytes(self.alloc, owned_values, version);
            try writes.append(self.alloc, .{ .key = member, .value = version });
            const guard = try makeReadGuardKey(self.alloc, predicate.key, txn_id);
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
            const count = std.math.add(u64, try self.readGuardCount(), next.count - previous.count) catch return error.TransactionTooLarge;
            const count_value = try self.alloc.alloc(u8, 8);
            try appendOwnedBytes(self.alloc, owned_values, count_value);
            std.mem.writeInt(u64, count_value[0..8], count, .little);
            try writes.append(self.alloc, .{ .key = read_guard_count_key, .value = count_value });
        }
        return .{ .previous = previous, .next = next };
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

    fn requireDocumentRevisionScope(self: *TxnManager, id: TxnId, scope: document_revision_reservation.Scope) !void {
        try scope.validate();
        if (!std.mem.eql(u8, &scope.transaction_id, &id)) return error.ExternalTransactionDecisionChanged;
        try self.requireNativeMutationPin(scope.native_pin);
        if (self.native_relation_pin != null and scope.native_pin == null) return error.ExternalTransactionDecisionChanged;
    }

    /// Ordered replay markers and the root's existing durable journal observer
    /// commit with the allocation, locks, watermark and pending record.
    pub const StagedDocumentRevisions = RevisionPlan;
    const RevisionPlan = struct {
        transaction_id: TxnId,
        arena: std.heap.ArenaAllocator,
        writes: std.ArrayList(docstore.KVPair) = .empty,
        versions: []const u64 = &.{},
        deletes: std.ArrayList([]const u8) = .empty,
        released: usize = 0,
        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const a = self.arena.allocator();
            try self.writes.append(a, .{ .key = try a.dupe(u8, key), .value = try a.dupe(u8, value) });
        }
        fn erase(self: *@This(), key: []const u8) !void {
            try self.deletes.append(self.arena.allocator(), try self.arena.allocator().dupe(u8, key));
        }
        /// Retain this owner through caller commit OR abort, even when stage
        /// fails after a partial put. The writer may borrow these buffers.
        pub fn stage(self: *const @This(), writer: anytype) !void {
            if (self.writes.items.len == 0 and self.deletes.items.len == 0) return;
            for (self.deletes.items) |key| try writer.delete(key);
            for (self.writes.items) |write| try writer.put(write.key, write.value);
            if (try topology_debt.load(writer)) |loaded| {
                var header = loaded;
                if (header.phase != .retired) {
                    try topology_debt.setMember(writer, &header, self.transaction_id, .blocked);
                    try header.bump();
                    try topology_debt.save(writer, header);
                }
            }
        }
    };

    fn planDocumentRevisions(self: *TxnManager, read: anytype, scope: document_revision_reservation.Scope, keys: []const []const u8, now: u64, expires_at: u64, subtransaction: u64) !RevisionPlan {
        try self.requireDocumentRevisionScope(scope.transaction_id, scope);
        const record_key = makeRecordKey(scope.transaction_id);
        const previous = try optionalTopologyValue(read, &record_key);
        var record = if (previous) |bytes| try decodeRecord(bytes) else TxnRecord{ .status = .pending, .begin_timestamp = scope.begin_timestamp, .created_at = now, .finalized_at = 0, .commit_version = 0 };
        if (record.status != .pending or record.prepared or record.intent_revision != 0 or record.external_decision_owner != null or
            record.begin_timestamp != scope.begin_timestamp or record.coordinator or record.retain_terminal) return error.DecisionConflict;
        if (try participantListCountInRead(read, &makeSidecarKey(participants_prefix, scope.transaction_id)) != 0) return error.DecisionConflict;
        const allocation = try document_revision_reservation.prepareScoped(self.alloc, read, scope, keys, now, expires_at, subtransaction);
        var result: RevisionPlan = .{ .transaction_id = scope.transaction_id, .arena = allocation.arena, .versions = allocation.versions };
        errdefer result.deinit();
        const a = result.arena.allocator();
        const lock_keys = try a.alloc([]const u8, keys.len);
        const owners = try a.alloc(?[]const u8, keys.len);
        const fresh = try a.alloc(bool, keys.len);
        @memset(fresh, false);
        for (allocation.fresh_indices) |index| fresh[index] = true;
        for (keys, lock_keys) |key, *lock| lock.* = try std.mem.concat(a, u8, &.{ intent_locks_prefix, key });
        try read.getManySorted(lock_keys, owners);
        for (owners, fresh) |held, is_new| {
            if (held) |id| {
                if (id.len != 16) return error.InvalidTxnRecord;
                if (!std.mem.eql(u8, id, &scope.transaction_id)) return error.IntentConflict;
                if (is_new) return error.InvalidTxnRecord;
            } else if (!is_new) return error.InvalidTxnRecord;
        }
        try result.writes.appendSlice(a, allocation.writes);
        for (allocation.fresh_indices) |index| try result.put(lock_keys[index], &scope.transaction_id);
        record.document_revisions = true;
        const encoded = try self.encodeRecord(record);
        defer self.alloc.free(encoded);
        try result.put(&record_key, encoded);
        if (previous == null) {
            const index_key = makeSidecarKey(participant_index_prefix, scope.transaction_id);
            var index_value: [17]u8 = undefined;
            (ParticipantIndex{}).encode(&index_value);
            try result.put(&index_key, &index_value);
        }
        const table_catalog = @import("db/table_catalog.zig");
        const previous_catalog = try optionalTopologyValue(read, table_catalog.key);
        var catalog = if (previous_catalog) |bytes| try table_catalog.Catalog.decode(bytes) else table_catalog.Catalog{};
        const effect_format: u8 = if (subtransaction == 0) 2 else 3;
        if (catalog.transaction_effect_format < effect_format) {
            catalog.transaction_effect_format = effect_format;
            const bytes = try catalog.encodeDurable();
            try result.put(table_catalog.key, bytes.slice());
        }
        return result;
    }

    /// Caller holds the primary apply fence and owns the accepting native
    /// writer. Command admission, allocation, row locks, replay marker and
    /// journal are staged in that SAME transaction. Abort it on every error.
    pub fn prepareDocumentRevisionsAtSubtransaction(self: *TxnManager, read: anytype, scope: document_revision_reservation.Scope, keys: []const []const u8, now: u64, expires_at: u64, subtransaction: u64) !StagedDocumentRevisions {
        return self.planDocumentRevisions(read, scope, keys, now, expires_at, subtransaction);
    }

    fn planDocumentRevisionRelease(self: *TxnManager, read: anytype, scope: document_revision_reservation.Scope, references: []const document_revision_reservation.Reference) !RevisionPlan {
        try self.requireDocumentRevisionScope(scope.transaction_id, scope);
        const bytes = (try optionalTopologyValue(read, &makeRecordKey(scope.transaction_id))) orelse return error.TxnNotFound;
        const record = try decodeRecord(bytes);
        if (record.status != .pending or record.prepared or record.intent_revision != 0 or record.external_decision_owner != null or record.begin_timestamp != scope.begin_timestamp) return error.DecisionConflict;
        try (document_revision_reservation.Handoff{ .scope = scope, .references = references }).validate();
        var result: RevisionPlan = .{ .transaction_id = scope.transaction_id, .arena = .init(self.alloc) };
        errdefer result.deinit();
        const a = result.arena.allocator();
        const keys = try a.alloc([]const u8, references.len);
        const lock_keys = try a.alloc([]const u8, references.len);
        const values = try a.alloc(?[]const u8, references.len);
        const owners = try a.alloc(?[]const u8, references.len);
        for (references, 0..) |reference, index| {
            keys[index] = try document_revision_reservation.rowKey(a, scope.transaction_id, reference.key);
            lock_keys[index] = try std.mem.concat(a, u8, &.{ intent_locks_prefix, reference.key });
        }
        var header = (try document_revision_reservation.load(read, scope.transaction_id)) orelse return error.ExternalTransactionDecisionChanged;
        if (!header.scope.eql(scope) or header.owner != null) return error.ExternalTransactionDecisionChanged;
        try read.getManySorted(keys, values);
        try read.getManySorted(lock_keys, owners);
        for (references, values, owners, 0..) |reference, value, owner, index| {
            if (value) |raw| {
                if (try document_revision_reservation.rowUsed(raw) or try document_revision_reservation.rowVersion(raw) != reference.version) return error.ExternalTransactionDecisionChanged;
                const actual = owner orelse return error.InvalidTxnRecord;
                if (actual.len != 16 or !std.mem.eql(u8, actual, &scope.transaction_id)) return error.InvalidTxnRecord;
                header.count = std.math.sub(u32, header.count, 1) catch return error.InvalidTxnRecord;
                header.key_bytes = std.math.sub(u32, header.key_bytes, @intCast(reference.key.len)) catch return error.InvalidTxnRecord;
                try result.erase(keys[index]);
                try result.erase(lock_keys[index]);
                result.released += 1;
            } else if (owner) |actual| {
                if (actual.len != 16 or std.mem.eql(u8, actual, &scope.transaction_id)) return error.InvalidTxnRecord;
            }
        }
        if (result.released != 0) try result.put(&document_revision_reservation.headerKey(scope.transaction_id), &try header.encode());
        return result;
    }

    pub fn prepareDocumentRevisionRelease(self: *TxnManager, read: anytype, scope: document_revision_reservation.Scope, references: []const document_revision_reservation.Reference) !StagedDocumentRevisions {
        return self.planDocumentRevisionRelease(read, scope, references);
    }

    pub fn prepareDocumentRevisionRenewal(self: *TxnManager, writer: anytype, scope: document_revision_reservation.Scope, now: u64, expires_at: u64) !StagedDocumentRevisions {
        // Empty release validates the same unadmitted record/header contract;
        // renewal cannot create a reservation or revive an expired one.
        var plan = try self.planDocumentRevisionRelease(writer, scope, &.{});
        errdefer plan.deinit();
        var header = (try document_revision_reservation.load(writer, scope.transaction_id)).?;
        if (now == 0 or expires_at <= now or expires_at - now > document_revision_reservation.max_lease_ns or header.expires_at <= now) return error.ExternalTransactionAdmissionExpired;
        if (header.expires_at < expires_at) {
            header.expires_at = expires_at;
            try plan.put(&document_revision_reservation.headerKey(scope.transaction_id), &try header.encode());
        }
        return plan;
    }

    /// The existing authenticated SQL source CLOSE retires only unadmitted
    /// first touches. Actual P custody always waits for its external decision.
    pub fn prepareAbandonDocumentRevisions(self: *TxnManager, writer: anytype, id: TxnId, principal: [32]u8, now: u64) !?StagedDocumentRevisions {
        const header = (try document_revision_reservation.load(writer, id)) orelse return null;
        if (!std.mem.eql(u8, &header.scope.principal_digest, &principal)) return error.LockOwnerChanged;
        try self.requireDocumentRevisionScope(id, header.scope);
        if (header.owner != null) return null;
        const raw = (try optionalTopologyValue(writer, &makeRecordKey(id))) orelse return error.InvalidTxnRecord;
        var record = try decodeRecord(raw);
        if (record.status == .aborted and record.intents_resolved_known and record.intents_resolved) return null;
        if (record.status != .pending or record.prepared or record.intent_revision != 0 or record.external_decision_owner != null or now == 0 or
            !record.document_revisions or record.begin_timestamp != header.scope.begin_timestamp) return error.DecisionConflict;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var references: std.ArrayList(document_revision_reservation.Reference) = .empty;
        {
            var cursor = try writer.openCursor();
            defer cursor.close();
            const prefix = document_revision_reservation.rowPrefix(id);
            var entry = try cursor.seekAtOrAfter(&prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, &prefix)) break;
                if (references.items.len >= header.count or references.items.len >= document_revision_reservation.max_rows or row.key.len == prefix.len) return error.InvalidTxnRecord;
                try references.append(a, .{ .key = try a.dupe(u8, row.key[prefix.len..]), .version = try document_revision_reservation.rowVersion(row.value) });
            }
        }
        if (references.items.len != header.count) return error.InvalidTxnRecord;
        var plan = try self.planDocumentRevisionRelease(writer, header.scope, references.items);
        errdefer plan.deinit();
        record.status = .aborted;
        record.finalized_at = now;
        record.intents_resolved = true;
        record.intents_resolved_known = true;
        const encoded = try self.encodeRecord(record);
        defer self.alloc.free(encoded);
        try plan.put(&makeRecordKey(id), encoded);
        return plan;
    }

    /// Existing source control delivery owns savepoint cleanup. The caller
    /// stages this plan in the same accepting writer as its control receipt.
    pub fn prepareDocumentRevisionSavepoint(self: *TxnManager, writer: anytype, id: TxnId, principal: [32]u8, action: @import("pessimistic_lock/ledger.zig").Control) !?StagedDocumentRevisions {
        if (action != .rollback and action != .release_savepoint) return null;
        const selected = switch (action) {
            .rollback => |value| value.transaction,
            .release_savepoint => |value| value.transaction,
            else => unreachable,
        };
        if (!std.mem.eql(u8, &id, &selected)) return error.InvalidLockScope;
        const header = (try document_revision_reservation.load(writer, id)) orelse return null;
        if (!std.mem.eql(u8, &header.scope.principal_digest, &principal)) return error.LockOwnerChanged;
        if (header.owner != null) return error.LockOwnerBusy;
        var plan = try self.planDocumentRevisionRelease(writer, header.scope, &.{});
        errdefer plan.deinit();
        const a = plan.arena.allocator();
        var references: std.ArrayList(document_revision_reservation.Reference) = .empty;
        var cursor = try writer.openCursor();
        defer cursor.close();
        const prefix = document_revision_reservation.rowPrefix(id);
        var entry = try cursor.seekAtOrAfter(&prefix);
        var count: usize = 0;
        var key_bytes: usize = 0;
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, &prefix)) break;
            count += 1;
            if (row.key.len == prefix.len or count > header.count) return error.InvalidTxnRecord;
            key_bytes = std.math.add(usize, key_bytes, row.key.len - prefix.len) catch return error.InvalidTxnRecord;
            if (key_bytes > header.key_bytes) return error.InvalidTxnRecord;
            if (try document_revision_reservation.rowUsed(row.value)) return error.LockOwnerBusy;
            const child = try document_revision_reservation.rowSubtransaction(row.value);
            switch (action) {
                .rollback => |rollback| if (child >= rollback.first_aborted) {
                    try references.append(a, .{ .key = try a.dupe(u8, row.key[prefix.len..]), .version = try document_revision_reservation.rowVersion(row.value) });
                },
                .release_savepoint => |release| {
                    if (release.first_child == 0 or release.parent >= release.first_child) return error.InvalidLockScope;
                    if (child >= release.first_child) {
                        const value = try document_revision_reservation.encodeRow(a, try document_revision_reservation.rowVersion(row.value), release.parent, false);
                        try plan.put(row.key, value);
                    }
                },
                else => unreachable,
            }
        }
        if (count != header.count or key_bytes != header.key_bytes) return error.InvalidTxnRecord;
        if (action == .rollback and references.items.len != 0) {
            const released = try self.planDocumentRevisionRelease(writer, header.scope, references.items);
            plan.deinit();
            return released;
        }
        return plan;
    }

    fn requireNativeMutationPin(self: *const TxnManager, requested: ?NativePhysicalPin) !void {
        const pin = requested orelse return;
        try pin.validate();
        const actual = self.native_relation_pin orelse return error.ExternalTransactionDecisionChanged;
        if (!actual.eql(pin)) return error.ExternalTransactionDecisionChanged;
    }

    pub fn requireUnpreparedSource(self: *TxnManager, txn_id: TxnId, purpose: [32]u8) !void {
        const record = try self.loadTransactionRecord(txn_id);
        if (!samePurpose(record.source_purpose, purpose)) return error.SourceTransactionIdentityConflict;
        if (record.status != .pending or !record.prepared_known or record.prepared or record.intent_revision != 0) return error.SourcePendingWork;
    }

    pub fn isLegacyProducer(self: *TxnManager, txn_id: TxnId) !bool {
        return (try self.loadTransactionRecord(txn_id)).legacy_producer;
    }

    pub fn isOrdinaryProducer(self: *TxnManager, txn_id: TxnId) !bool {
        return (try self.loadTransactionRecord(txn_id)).ordinary_producer;
    }

    pub fn getSourceScope(self: *TxnManager, txn_id: TxnId) !?source_scope.Reference {
        return (try self.loadTransactionRecord(txn_id)).source_scope;
    }

    pub fn getSourcePurpose(self: *TxnManager, txn_id: TxnId) !?[32]u8 {
        return (try self.loadTransactionRecord(txn_id)).source_purpose;
    }

    pub fn requireSourcePurpose(self: *TxnManager, txn_id: TxnId, purpose: [32]u8) !void {
        const record = try self.loadTransactionRecord(txn_id);
        if (!samePurpose(record.source_purpose, purpose)) return error.SourceTransactionIdentityConflict;
    }

    fn requireSamePreparedSourceIntents(self: *TxnManager, txn_id: TxnId, intents: []const WriteIntent) !void {
        var incoming = std.StringHashMapUnmanaged(WriteIntent).empty;
        defer incoming.deinit(self.alloc);
        for (intents) |intent| try incoming.put(self.alloc, intent.key, intent);
        var previous = try self.collectIntentBatch(self.alloc, txn_id);
        defer previous.deinit(self.alloc);
        const entries = previous.owned_entries orelse {
            if (incoming.count() != 0) return error.SourcePayloadConflict;
            return;
        };
        if (entries.len != incoming.count()) return error.SourcePayloadConflict;
        for (entries) |entry| {
            const value = try IntentValue.decode(entry.value);
            const next = incoming.get(entry.key[intents_prefix.len + 17 ..]) orelse return error.SourcePayloadConflict;
            if (!sameBytes(value.value, next.value) or !sameBytes(value.prepared_row, next.prepared_row)) return error.SourcePayloadConflict;
        }
    }

    fn loadTransactionRecord(self: *TxnManager, txn_id: TxnId) !TxnRecord {
        const key = makeRecordKey(txn_id);
        const val = self.getAlloc(self.alloc, &key) catch |err| switch (err) {
            error.NotFound => return TxnError.TxnNotFound,
            else => return err,
        };
        defer self.alloc.free(val);
        const record = try decodeRecord(val);
        if (self.native_relation_pin) |actual| {
            if (record.external_decision_owner != null) {
                const pin = record.native_pin orelse return error.ExternalTransactionDecisionChanged;
                if (!pin.eql(actual)) return error.ExternalTransactionDecisionChanged;
            }
        }
        return record;
    }

    fn saveTransactionRecord(self: *TxnManager, key: [records_prefix.len + 16]u8, record: TxnRecord) !void {
        const encoded = try self.encodeRecord(record);
        defer self.alloc.free(encoded);
        try self.putValue(&key, encoded);
    }

    fn encodeRecord(self: *TxnManager, record: TxnRecord) ![]u8 {
        if (record.native_pin != null and record.external_decision_owner == null) return error.InvalidTxnRecord;
        if (record.replication_source_required and (record.external_decision_owner == null or record.native_pin != null or
            (!record.prepared and record.status != .aborted))) return error.InvalidTxnRecord;
        if (record.publication_capture_required and (record.external_decision_owner == null or record.native_pin != null or !record.prepared)) return error.InvalidTxnRecord;
        if (self.native_relation_pin) |actual| if (record.external_decision_owner != null) {
            const pin = record.native_pin orelse return error.ExternalTransactionDecisionChanged;
            if (!pin.eql(actual)) return error.ExternalTransactionDecisionChanged;
        };
        if ((record.ordinary_producer and record.legacy_producer) or ((record.ordinary_producer or record.legacy_producer) and (record.source_purpose != null or record.source_scope != null))) return error.InvalidTxnRecord;
        if (record.source_scope != null and (record.source_purpose == null or !record.retain_terminal)) return error.InvalidTxnRecord;
        if (record.external_decision_owner != null and
            (record.legacy_producer or record.source_purpose != null or record.source_scope != null or
                (record.ordinary_producer and (record.native_pin != null or record.replication_source_required or !record.retain_terminal))))
            return error.InvalidTxnRecord;
        const size: usize = if (record.native_pin != null)
            txn_record_external_pinned_size
        else if (record.external_decision_owner != null)
            txn_record_external_size
        else if (record.ordinary_producer or record.legacy_producer)
            txn_record_v10_size
        else if (record.source_scope != null)
            txn_record_v9_size
        else if (record.source_purpose != null)
            txn_record_v8_size
        else if (record.document_revisions)
            txn_record_v7_size
        else
            txn_record_v6_size;
        const buf = try self.alloc.alloc(u8, size);
        errdefer self.alloc.free(buf);
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
        if (record.external_decision_owner) |owner| {
            // Data Raft protocol 72 owns the external completion provenance bit.
            buf[txn_record_v6_size] = @as(u8, @intFromBool(record.document_revisions)) |
                (@as(u8, @intFromBool(record.publication_capture_required)) << 2) |
                (@as(u8, @intFromBool(record.replication_source_required)) << 3) |
                (@as(u8, @intFromBool(record.ordinary_producer)) << 4);
            if ((record.external_commit_started and !record.prepared) or !record.coordinator_known or
                (record.status == .committed and !record.external_commit_started) or
                (record.status == .aborted and record.external_commit_started)) return error.InvalidTxnRecord;
            @memcpy(buf[txn_record_v7_size..][0..ExternalDecisionOwner.encoded_len], &try owner.encode());
            buf[txn_record_external_size - 1] = @intFromBool(record.external_commit_started);
            if (record.native_pin) |pin| @memcpy(buf[txn_record_external_size..][0..NativePhysicalPin.encoded_len], &try pin.encode());
        } else if (record.ordinary_producer or record.legacy_producer) {
            buf[53] = @intFromBool(record.document_revisions);
            // v10 ordinary remains byte-for-byte stable. Protocol 44 adds
            // v11 legacy discriminator 2, rejected by earlier decoders.
            buf[54] = if (record.ordinary_producer) 1 else 2;
        } else if (record.source_purpose) |purpose| {
            buf[53] = @intFromBool(record.document_revisions);
            buf[54] = if (record.source_scope != null) 2 else 1; // scoped or legacy source delivery
            @memcpy(buf[55..87], &purpose);
            if (record.source_scope) |scope| @memcpy(buf[87..txn_record_v9_size], &try scope.encode());
        } else if (record.document_revisions) buf[53] = 1;
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
        return self.cleanupTransactionMetadataWithRetirement(txn_id, cutoff_timestamp, retained_cutoff_timestamp, extra_batch, null);
    }

    /// A closed source grant has its own persistent retry fence. Its aborted
    /// journal can be pruned immediately, after all intents/outboxes/ACK debt
    /// drain, instead of accumulating one retained record per replan attempt.
    pub fn cleanupRetiredSourceAttempt(self: *TxnManager, command: @import("source_admission.zig").Command) !bool {
        try command.validate();
        {
            var probe = try self.store.beginProbe();
            defer probe.abort();
            const fence = (try @import("source_admission.zig").loadFence(&probe, command)) orelse return error.SourcePendingWork;
            if (fence.table_id != command.table_id or !std.mem.eql(u8, &fence.binding, &command.binding())) return error.SourceBindingConflict;
            if (fence.sequence < command.source_sequence) return error.SourcePendingWork;
            if (fence.sequence == command.source_sequence and (fence.closed_through < command.attempt or
                !std.mem.eql(u8, &fence.logical_identity, &command.logicalIdentity()) or
                !std.mem.eql(u8, &fence.input_digest, &command.inputFingerprint()))) return error.SourcePendingWork;
        }
        return self.cleanupTransactionMetadataWithRetirement(command.transactionId(), 0, 0, .{}, command.fingerprint());
    }

    fn cleanupTransactionMetadataWithRetirement(
        self: *TxnManager,
        txn_id: TxnId,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
        extra_batch: MutationExtraBatch,
        retired_source: ?[32]u8,
    ) !bool {
        try self.requireNativeMutationPin(extra_batch.native_pin);
        const record = self.loadTransactionRecord(txn_id) catch |err| switch (err) {
            TxnError.TxnNotFound => {
                try self.applyMutationExtraBatch(extra_batch);
                return false;
            },
            else => return err,
        };
        if (retired_source) |purpose| {
            if (!samePurpose(record.source_purpose, purpose)) return error.SourceTransactionIdentityConflict;
            if (record.status != .aborted) return error.SourcePendingWork;
        }
        const has_intents = if (record.intents_resolved_known and record.intents_resolved)
            false
        else
            try self.hasAnyIntents(txn_id);
        if (record.status == .pending or has_intents or try self.hasReplicationOutbox(txn_id)) {
            try self.applyMutationExtraBatch(extra_batch);
            return false;
        }
        if (record.status == .committed) {
            if (try self.getCompletionAdmission(self.alloc, txn_id)) |saved| {
                var frozen = saved;
                defer frozen.deinit();
                // Publication/retention handoff must explicitly retire this
                // debt. Ordinary terminal-record age is not completion proof.
                try self.applyMutationExtraBatch(extra_batch);
                return false;
            }
        }
        if (retired_source == null and !terminalRetentionElapsed(record, cutoff_timestamp, retained_cutoff_timestamp)) {
            if (record.status == .aborted) {
                if (try self.getCompletionAdmission(self.alloc, txn_id)) |saved| {
                    var frozen = saved;
                    defer frozen.deinit();
                    const unresolved = try self.getUnresolvedParticipants(self.alloc, txn_id);
                    defer freeParticipantList(self.alloc, unresolved);
                    if (unresolved.len == 0) {
                        // Retain the terminal retry fence, but an all-aborted
                        // cohort needs no future committed certificate.
                        const plan_key = makeSidecarKey(completion_admission_prefix, txn_id);
                        var deletes: std.ArrayList([]const u8) = .empty;
                        defer deletes.deinit(self.alloc);
                        try deletes.appendSlice(self.alloc, extra_batch.deletes);
                        try deletes.append(self.alloc, &plan_key);
                        try self.applyBatch(extra_batch.writes, deletes.items, null);
                        return false;
                    }
                }
            }
            try self.applyMutationExtraBatch(extra_batch);
            return false;
        }
        if (!try self.nativeLockCleanupReady(txn_id)) {
            try self.applyMutationExtraBatch(extra_batch);
            return false;
        }
        if (record.replication_source_required and !record.prepared) {
            var read = try self.store.beginProbe();
            defer read.abort();
            const key = makeSidecarKey(participants_prefix, txn_id);
            if (try participantListCountInRead(&read, &key) == 0) {
                try self.applyMutationExtraBatch(extra_batch);
                return false;
            }
        }
        const unresolved = try self.getUnresolvedParticipants(self.alloc, txn_id);
        defer freeParticipantList(self.alloc, unresolved);
        if (unresolved.len != 0) {
            try self.applyMutationExtraBatch(extra_batch);
            return false;
        }
        try self.deleteTransactionMetadataKnownRecord(txn_id, record, extra_batch);
        return true;
    }

    fn hasAnyIntents(self: *TxnManager, txn_id: TxnId) !bool {
        return try self.hasIntents(txn_id);
    }

    fn loadIntentKeysOptional(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !?[][]u8 {
        if (try self.loadIntentAdmission(alloc, txn_id)) |admission| {
            const prefix = try makeIntentMemberKey(alloc, txn_id, "");
            defer alloc.free(prefix);
            // Admission already owns the exact member count. Allocate once,
            // avoiding growth and a backend/address-dependent shrink/remap.
            const count = std.math.cast(usize, admission.count) orelse return error.InvalidTxnRecord;
            const keys = try alloc.alloc([]u8, count);
            var initialized: usize = 0;
            errdefer {
                for (keys[0..initialized]) |key| alloc.free(key);
                alloc.free(keys);
            }
            var scan = try self.store.beginCurrentScan();
            defer scan.abort();
            var cursor = try scan.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |item| {
                if (!std.mem.startsWith(u8, item.key, prefix)) break;
                if (initialized == keys.len) return error.IntentSnapshotChanged;
                keys[initialized] = try alloc.dupe(u8, item.key[prefix.len..]);
                initialized += 1;
                entry = try cursor.next();
            }
            if (initialized != keys.len) return error.IntentSnapshotChanged;
            return keys;
        }
        const key = makeSidecarKey(intent_keys_prefix, txn_id);
        const raw = self.getAlloc(alloc, &key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(raw);
        return try decodeParticipantList(alloc, raw);
    }

    fn loadIntentAdmission(self: *TxnManager, alloc: Allocator, txn_id: TxnId) !?IntentAdmission {
        const key = makeSidecarKey(intent_admission_prefix, txn_id);
        const raw = self.getAlloc(alloc, &key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(raw);
        if (raw.len != 16 and raw.len != 32) return error.InvalidTxnRecord;
        return .{ .count = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u64, raw[8..16], .little), .retained_bytes = if (raw.len == 32) std.mem.readInt(u64, raw[16..24], .little) else 0, .retained_keys = if (raw.len == 32) std.mem.readInt(u64, raw[24..32], .little) else 0, .retention_tracked = raw.len == 32 };
    }

    fn makeIntentMemberKey(alloc: Allocator, txn_id: TxnId, key: []const u8) ![]u8 {
        return try std.mem.concat(alloc, u8, &.{ intent_members_prefix, &txn_id, ":", key });
    }

    fn loadIntentEntries(
        self: *TxnManager,
        alloc: Allocator,
        txn_id: TxnId,
        scan_prefix: []const u8,
    ) ![]backend_scan.OwnedKVPair {
        const intent_keys = (try self.loadIntentKeysOptional(alloc, txn_id)) orelse
            return try self.scanPrefix(alloc, scan_prefix);
        defer freeParticipantList(alloc, intent_keys);
        return try self.loadIntentEntriesByKeys(alloc, scan_prefix, intent_keys);
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
        if (user_keys.len == 0) return try alloc.alloc(backend_scan.OwnedKVPair, 0);

        const full_keys = try alloc.alloc([]u8, user_keys.len);
        var full_keys_initialized: usize = 0;
        defer {
            for (full_keys[0..full_keys_initialized]) |key| alloc.free(key);
            alloc.free(full_keys);
        }
        for (user_keys, 0..) |user_key, i| {
            full_keys[i] = try std.mem.concat(alloc, u8, &.{ scan_prefix, user_key });
            full_keys_initialized += 1;
        }
        std.mem.sort([]u8, full_keys, {}, struct {
            fn lessThan(_: void, left: []u8, right: []u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        }.lessThan);

        const key_refs = try alloc.alloc([]const u8, full_keys.len);
        defer alloc.free(key_refs);
        for (full_keys, 0..) |key, i| key_refs[i] = key;
        const values = try alloc.alloc(?[]const u8, full_keys.len);
        defer alloc.free(values);
        @memset(values, null);
        var probe = try self.store.beginProbe();
        defer probe.abort();
        try probe.getManySorted(key_refs, values);

        const entries = try alloc.alloc(backend_scan.OwnedKVPair, full_keys.len);
        var initialized: usize = 0;
        errdefer {
            for (entries[0..initialized]) |entry| {
                alloc.free(entry.key);
                alloc.free(entry.value);
            }
            alloc.free(entries);
        }
        for (values, 0..) |maybe_value, i| {
            const value = maybe_value orelse return error.IntentSnapshotChanged;
            const owned_key = try alloc.dupe(u8, key_refs[i]);
            errdefer alloc.free(owned_key);
            const owned_value = try alloc.dupe(u8, value);
            entries[i] = .{
                .key = owned_key,
                .value = owned_value,
            };
            initialized += 1;
        }
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
        return self.deleteTransactionMetadataKnownRecord(txn_id, try self.loadTransactionRecord(txn_id), extra_batch);
    }

    fn nativeLockCleanupReady(self: *TxnManager, txn_id: TxnId) !bool {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        var read = try self.store.beginRead();
        defer read.abort();
        return @import("pessimistic_lock/transaction.zig").cleanupReady(arena.allocator(), &read, txn_id);
    }

    fn deleteTransactionMetadataKnownRecord(self: *TxnManager, txn_id: TxnId, record: TxnRecord, extra_batch: MutationExtraBatch) !void {
        // All deletion paths retain the native decision until lock retirement,
        // including bulk GC and direct cleanup after coordinator ACK.
        if (!try self.nativeLockCleanupReady(txn_id)) return error.LockOwnerBusy;
        if (try self.getCompletionAdmission(self.alloc, txn_id)) |saved| {
            var frozen = saved;
            defer frozen.deinit();
            if (record.status != .aborted) return TxnError.DecisionConflict;
        }
        const record_key = makeRecordKey(txn_id);
        const participant_key = makeSidecarKey(participants_prefix, txn_id);
        const resolved_key = makeSidecarKey(resolved_participants_prefix, txn_id);
        const hot_standby_batch_key = makeTransactionReplicationBatchOutboxKey(txn_id);
        const hot_standby_replay_key = makeTransactionReplicationReplayOutboxKey(txn_id);
        const intent_keys_key = makeSidecarKey(intent_keys_prefix, txn_id);
        const schema_lease_key = makeSidecarKey(schema_leases_prefix, txn_id);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);
        const lock_key = @import("pessimistic_lock/transaction.zig").key(txn_id);
        try deletes.append(self.alloc, &lock_key);
        const admission_key = makeSidecarKey(intent_admission_prefix, txn_id);
        try deletes.append(self.alloc, &admission_key);
        const completion_admission_key = makeSidecarKey(completion_admission_prefix, txn_id);
        try deletes.append(self.alloc, &completion_admission_key);
        try deletes.appendSlice(self.alloc, &.{ &record_key, &participant_key, &resolved_key, &hot_standby_batch_key, &hot_standby_replay_key, &intent_keys_key, &schema_lease_key });
        try deletes.appendSlice(self.alloc, extra_batch.deletes);
        const member_prefix = makeSidecarKey(participant_index_prefix, txn_id);
        const resolution_prefix = makeSidecarKey(resolved_index_prefix, txn_id);
        const members = try self.scanPrefix(self.alloc, &member_prefix);
        defer backend_scan.freeResults(self.alloc, members);
        const resolutions = try self.scanPrefix(self.alloc, &resolution_prefix);
        defer backend_scan.freeResults(self.alloc, resolutions);
        for (members) |member| try deletes.append(self.alloc, member.key);
        for (resolutions) |resolution| try deletes.append(self.alloc, resolution.key);
        const revision_header: ?document_revision_reservation.Header = if (record.document_revisions) blk: {
            var probe = try self.store.beginProbe();
            defer probe.abort();
            break :blk try document_revision_reservation.load(&probe, txn_id);
        } else null;
        var revision_cleanup: ?document_revision_reservation.Cleanup = null;
        defer if (revision_cleanup) |*cleanup| cleanup.deinit();
        if (revision_header) |header| {
            var scan = try self.store.beginCurrentScan();
            defer scan.abort();
            revision_cleanup = try document_revision_reservation.prepareCleanup(self.alloc, &scan, header);
            try deletes.appendSlice(self.alloc, revision_cleanup.?.keys);
        }
        var writes: std.ArrayListUnmanaged(docstore.KVPair) = .empty;
        defer writes.deinit(self.alloc);
        try writes.appendSlice(self.alloc, extra_batch.writes);
        var floor_value: [8]u8 = undefined;
        if (record.external_decision_owner) |owner| if (owner.expires_at_unix_ns > try self.externalExpiryFloor()) {
            std.mem.writeInt(u64, &floor_value, owner.expires_at_unix_ns, .little);
            try writes.append(self.alloc, .{ .key = external_expiry_floor_key, .value = &floor_value });
        };
        try self.applyBatch(writes.items, deletes.items, null);
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

    pub fn getAlloc(self: *TxnManager, alloc: Allocator, key: []const u8) ![]u8 {
        // These helpers copy one value and release it immediately; none of
        // their callers retain a multi-operation snapshot. On the runtime LSM
        // a bound read clones the mutable memtable, while a probe reads the
        // current tip under the backend lock. In particular, ordinary batch
        // admission calls this for every transaction-intent lock key.
        var txn = try self.store.beginProbe();
        defer txn.abort();
        const value = try txn.get(key);
        return try alloc.dupe(u8, value);
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
        try self.applyBatch(&.{.{ .key = key, .value = value }}, &.{}, null);
    }

    fn applyMutationExtraBatch(self: *TxnManager, extra_batch: MutationExtraBatch) !void {
        if (extra_batch.writes.len == 0 and extra_batch.deletes.len == 0) return;
        try self.applyBatch(extra_batch.writes, extra_batch.deletes, null);
    }

    fn applyMutationExtraBatchStaged(self: *TxnManager, extra_batch: MutationExtraBatch) !backend_erased.Ticket {
        if (extra_batch.writes.len == 0 and extra_batch.deletes.len == 0) return backend_erased.Ticket.alreadyDurable();
        return try self.applyBatchStaged(extra_batch.writes, extra_batch.deletes, null, null, null);
    }

    fn applyBatch(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend) !void {
        return self.applyBatchWithReservation(writes, deletes, replay, null);
    }

    const Capture = struct { id: TxnId, sink: @import("publication_prepare_sink.zig").Sink };
    const ReservationChange = struct {
        previous: u64,
        next: u64,
        capture: ?Capture = null,
        terminal: ?@import("publication_prepare_sink.zig").TerminalSink = null,
        source_ack: ?ReplicationAcknowledgement = null,
        source_resolution: ?struct { input: ReplicationOrderedPhase, revision: u64, status: TxnStatus } = null,
        publication: ?struct {
            source: ?ReplicationPrepare = null,
            input: ?@import("db/logical_replication_outbox.zig").Preparation,
            id: TxnId,
            revision: u64,
            intents: []const WriteIntent,
        } = null,
    };

    fn stagePrepareRoster(a: Allocator, batch: anytype, id: TxnId) !void {
        // BEGIN can predate capture admission. Emit the actual roster so a
        // follower can install the complete PREPARE without replaying BEGIN.
        const participants = makeSidecarKey(participants_prefix, id);
        if (try optionalTopologyValue(batch, &participants)) |bytes| {
            const members = try decodeParticipantList(a, bytes);
            defer freeParticipantList(a, members);
            try batch.put(&participants, bytes);
            for (members) |member| {
                const key = makeParticipantIndexKey(participant_index_prefix, id, member);
                const held = (try optionalTopologyValue(batch, &key)) orelse return error.InvalidTxnRecord;
                if (!std.mem.eql(u8, held, member)) return error.InvalidTxnRecord;
                try batch.put(&key, held);
            }
        }
        const index = makeSidecarKey(participant_index_prefix, id);
        try batch.put(&index, (try optionalTopologyValue(batch, &index)) orelse return error.InvalidTxnRecord);
    }
    fn applyBatchWithReservation(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange) !void {
        return self.applyBatchWithParticipant(writes, deletes, replay, reservation, null);
    }

    fn applyBatchWithParticipant(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange, participant: ?@import("commit_participant.zig").Participant) !void {
        var ticket = try self.applyBatchStaged(writes, deletes, replay, reservation, participant);
        try ticket.wait();
    }

    /// Staged counterpart of `applyBatchWithParticipant`: stages the batch
    /// and returns a durability ticket instead of blocking for the fsync.
    /// `backend_erased.Batch.commitStaged` is already backend-agnostic (see
    /// its own doc comment): a backend without a staged commit path, e.g.
    /// the hosted LSM backend, returns an already-durable ticket from the
    /// ordinary blocking commit, so this never changes behavior there.
    fn applyBatchStaged(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange, participant: ?@import("commit_participant.zig").Participant) !backend_erased.Ticket {
        return self.applyBatchStagedWithHistory(writes, deletes, replay, reservation, participant, null, null, null);
    }

    fn applyBatchStagedWithHistory(self: *TxnManager, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange, participant: ?@import("commit_participant.zig").Participant, scope_cut: ?source_scope.Cut, source_retirement: ?SourceRetirement, legacy_terminal: ?LegacyTerminalResolution) !backend_erased.Ticket {
        if (reservation) |change| if (change.capture) |capture| {
            if (replay != null or participant != null) return error.InvalidCommitMetadata;
            return self.applyBatchStagedWithCapture(true, writes, deletes, replay, reservation, participant, capture, scope_cut, source_retirement, legacy_terminal);
        };
        return self.applyBatchStagedWithCapture(false, writes, deletes, replay, reservation, participant, undefined, scope_cut, source_retirement, legacy_terminal);
    }

    fn applyBatchStagedWithCapture(self: *TxnManager, comptime recording: bool, writes: []const docstore.KVPair, deletes: []const []const u8, replay: ?ReplayAppend, reservation: ?ReservationChange, participant: ?@import("commit_participant.zig").Participant, capture: Capture, scope_cut: ?source_scope.Cut, source_retirement: ?SourceRetirement, legacy_terminal: ?LegacyTerminalResolution) !backend_erased.Ticket {
        // Transaction extras are not a directory initialization/repair API.
        for (writes) |kv| if (std.mem.startsWith(u8, kv.key, topology_debt.family_prefix)) return error.InvalidCommitMetadata;
        for (deletes) |key| if (std.mem.startsWith(u8, key, topology_debt.family_prefix)) return error.InvalidCommitMetadata;
        var raw_batch = try self.store.beginBatch();
        errdefer raw_batch.abort();
        var arena = if (recording) std.heap.ArenaAllocator.init(self.alloc) else {};
        defer if (recording) arena.deinit();
        var recorder = if (recording) @import("publication_prepare_sink.zig").Recording(@TypeOf(raw_batch)){
            .inner = &raw_batch,
            .alloc = arena.allocator(),
            .maximum = capture.sink.max_bytes,
        } else {};
        const batch = if (recording) &recorder else &raw_batch;
        if (recording) try stagePrepareRoster(arena.allocator(), batch, capture.id);
        var directory = try topology_debt.load(batch);
        const CommitGuard = struct {
            original: @import("commit_participant.zig").Participant,
            protected: bool,
            fn reset(ptr: *anyopaque) void {
                const observer: *@This() = @ptrCast(@alignCast(ptr));
                observer.original.reset(observer.original.ptr);
            }
            fn observe(ptr: *anyopaque, key: []const u8, value: ?[]const u8) void {
                const observer: *@This() = @ptrCast(@alignCast(ptr));
                observer.original.observe(observer.original.ptr, key, value);
            }
            const GuardedView = struct {
                original: @import("commit_participant.zig").View,
                fn get(raw: *anyopaque, key: []const u8) ![]const u8 {
                    const guard: *@This() = @ptrCast(@alignCast(raw));
                    return guard.original.get(key);
                }
                fn put(raw: *anyopaque, key: []const u8, value: []const u8) !void {
                    const guard: *@This() = @ptrCast(@alignCast(raw));
                    // Final callbacks cannot change transaction evidence
                    // after its directory update or mint a ready marker.
                    if (std.mem.startsWith(u8, key, "\x00\x00__txn_") or std.mem.startsWith(u8, key, topology_debt.family_prefix)) return error.InvalidCommitMetadata;
                    try guard.original.put(key, value);
                }
            };
            fn stage(ptr: *anyopaque, view: @import("commit_participant.zig").View, position: []const u8) anyerror!void {
                const observer: *@This() = @ptrCast(@alignCast(ptr));
                if (!observer.protected) return observer.original.stage(observer.original.ptr, view, position);
                var guarded: GuardedView = .{ .original = view };
                try observer.original.stage(observer.original.ptr, .{ .ptr = &guarded, .get_fn = GuardedView.get, .put_fn = GuardedView.put }, position);
            }
        };
        var commit_guard: CommitGuard = undefined;
        if (participant) |observer| {
            commit_guard = .{ .original = observer, .protected = directory != null and directory.?.phase != .retired };
            try batch.setCommitParticipant(.{ .ptr = &commit_guard, .stage_control_on_empty = observer.stage_control_on_empty, .reset = CommitGuard.reset, .observe = CommitGuard.observe, .stage = CommitGuard.stage });
        }
        var capture_scratch = if (recording) {} else std.heap.ArenaAllocator.init(self.alloc);
        defer if (!recording) capture_scratch.deinit();
        if (reservation) |change| {
            if (change.source_ack == null) try retained_effects.replaceReservation(batch, change.previous, change.next);
            if (change.publication) |publication| {
                const outbox = @import("db/logical_replication_outbox.zig");
                if (publication.input) |input| {
                    const scratch = if (recording) arena.allocator() else capture_scratch.allocator();
                    _ = try outbox.stage(scratch, batch, input, publication.id, publication.revision, publication.intents);
                } else try outbox.requireUncapturedIntents(self.alloc, batch, publication.intents);
            }
        }
        var scope_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer scope_arena.deinit();
        // Producer closure and transaction admission share this exact native
        // writer. Metadata-only ACK/cleanup does not create new row authority.
        const producer_policy = @import("source_producer_policy.zig");
        const producer_closed = try producer_policy.Store.readStored(scope_arena.allocator(), batch);
        if (legacy_terminal) |proof| {
            // Terminal retries can stage only markers/receipts, with no record
            // replacement in writes. Their authority must still be checked in
            // this exact native writer, including a concurrent GC/reuse race.
            try proof.validate();
            const closed = producer_closed orelse return error.SourcePendingWork;
            if (!std.meta.eql(closed.root, proof.root)) return error.SourceTransactionIdentityConflict;
            const raw = batch.get(&makeRecordKey(proof.transaction_id)) catch |err| return if (err == error.NotFound) error.TxnNotFound else err;
            if (!std.mem.eql(u8, &completion.bytesDigest(raw), &proof.record_digest)) return error.SourceTransactionIdentityConflict;
            const original = try decodeRecord(raw);
            if (original.status == .pending or original.ordinary_producer or original.source_scope != null or original.finalized_at == 0 or (original.status == .committed and original.commit_version == 0)) return error.SourceOutcomeUnresolved;
        }
        // Scope state and the retained record/effects share this physical commit.
        // Inspect original records before staging any replacement record bytes.
        for (writes) |write| {
            if (write.key.len != records_prefix.len + 16 or !std.mem.startsWith(u8, write.key, records_prefix)) continue;
            const next_record = try decodeRecord(write.value);
            const previous_bytes = batch.get(write.key) catch |err| switch (@as(anyerror, err)) {
                error.NotFound => null,
                else => return err,
            };
            if (previous_bytes) |bytes| {
                const previous = try decodeRecord(bytes);
                if (previous.ordinary_producer != next_record.ordinary_producer or previous.legacy_producer != next_record.legacy_producer) return error.SourceTransactionIdentityConflict;
            }
            if (producer_closed != null) {
                // Plain/legacy records cannot acquire authority after closure.
                if (next_record.source_scope == null and !next_record.ordinary_producer) {
                    const proof = legacy_terminal orelse return error.SourcePendingWork;
                    const id: TxnId = write.key[records_prefix.len..][0..16].*;
                    if (!std.mem.eql(u8, &id, &proof.transaction_id) or !std.meta.eql(producer_closed.?.root, proof.root)) return error.SourceTransactionIdentityConflict;
                    const original_bytes = previous_bytes orelse return error.SourceOutcomeUnresolved;
                    if (!std.mem.eql(u8, &completion.bytesDigest(original_bytes), &proof.record_digest)) return error.SourceTransactionIdentityConflict;
                    var original = try decodeRecord(original_bytes);
                    if (original.status == .pending or original.ordinary_producer or original.source_scope != null or original.finalized_at == 0 or (original.status == .committed and original.commit_version == 0)) return error.SourceOutcomeUnresolved;
                    // Resolution may retire intents and advance only their
                    // replay marker, preserving every original decision field.
                    original.intents_resolved = next_record.intents_resolved;
                    original.intents_resolved_known = next_record.intents_resolved_known;
                    original.replay_sequence = next_record.replay_sequence;
                    if (!std.meta.eql(original, next_record) or !next_record.intents_resolved_known or !next_record.intents_resolved) return error.SourceTransactionIdentityConflict;
                }
                if (next_record.ordinary_producer) {
                    const id: TxnId = write.key[records_prefix.len..][0..16].*;
                    const admission_key = makeSidecarKey(completion_admission_prefix, id);
                    var supplied: ?[]const u8 = null;
                    for (writes) |candidate| if (std.mem.eql(u8, candidate.key, &admission_key)) {
                        if (supplied != null) return error.InvalidTxnRecord;
                        supplied = candidate.value;
                    };
                    var parsed = try completion.parseAdmission(scope_arena.allocator(), supplied orelse try batch.get(&admission_key));
                    defer parsed.deinit();
                    const admission = parsed.value.admission;
                    if (admission.version < 3 or !std.mem.eql(u8, &admission.transaction_id, &id)) return error.InvalidTxnRecord;
                    const namespace = try batch.get(&internal_keys.identity_namespace_key);
                    if (namespace.len != 24) return error.InvalidTxnRecord;
                    const member = try parsed.value.localMember(namespace[0..24].*);
                    if (next_record.coordinator != admission.isCoordinator(member)) return error.InvalidParticipant;
                    try producer_policy.Store.require(scope_arena.allocator(), batch, try source_scope.Basis.fromOwner(member.routed_owner.?), .ordinary);
                }
            }
            if (next_record.legacy_producer and previous_bytes == null) try source_scope.Store.requireLegacyAllowed(scope_arena.allocator(), batch);
            if (write.value.len == txn_record_v8_size) {
                const legacy = try decodeRecord(write.value);
                if (legacy.status == .pending and legacy.source_purpose != null) {
                    const exists = blk: {
                        _ = batch.get(write.key) catch |err| switch (@as(anyerror, err)) {
                            error.NotFound => break :blk false,
                            else => return err,
                        };
                        break :blk true;
                    };
                    if (!exists) try source_scope.Store.requireLegacyAllowed(scope_arena.allocator(), batch);
                }
                continue;
            }
            if (write.value.len != txn_record_v9_size) continue;
            const next = try decodeRecord(write.value);
            const reference = next.source_scope orelse return error.InvalidTxnRecord;
            const prior_bytes = batch.get(write.key) catch |err| switch (@as(anyerror, err)) {
                error.NotFound => null,
                else => return err,
            };
            const prior = if (prior_bytes) |bytes| try decodeRecord(bytes) else null;
            if (prior) |old| {
                if (!sameSourceScope(old.source_scope, next.source_scope) or !samePurpose(old.source_purpose, next.source_purpose)) return error.SourceTransactionIdentityConflict;
                if (old.status != .pending and old.intents_resolved_known and old.intents_resolved) continue;
            }
            const id: TxnId = write.key[records_prefix.len..][0..16].*;
            const admission_key = makeSidecarKey(completion_admission_prefix, id);
            var admission_bytes: ?[]const u8 = null;
            for (writes) |candidate| if (std.mem.eql(u8, candidate.key, &admission_key)) {
                if (admission_bytes != null) return error.InvalidTxnRecord;
                admission_bytes = candidate.value;
            };
            const retained = admission_bytes orelse try batch.get(&admission_key);
            var parsed = try completion.parseAdmission(scope_arena.allocator(), retained);
            defer parsed.deinit();
            const admission = parsed.value.admission;
            if (admission.version < 3 or !std.mem.eql(u8, &admission.transaction_id, &id)) return error.InvalidTxnRecord;
            const namespace = try batch.get(&internal_keys.identity_namespace_key);
            if (namespace.len != 24) return error.InvalidTxnRecord;
            const member = try parsed.value.localMember(namespace[0..24].*);
            const coordinator = try admission.coordinatorMember();
            if (coordinator.roles & completion.Roles.checkpoint == 0 or next.coordinator != admission.isCoordinator(member)) return error.InvalidParticipant;
            const root = try source_scope.Basis.fromOwner(member.routed_owner.?);
            try producer_policy.Store.require(scope_arena.allocator(), batch, root, .scoped_source);
            const coordinator_root = try source_scope.Basis.fromOwner(coordinator.routed_owner.?);
            const slot: source_scope.Slot = .{ .reference = reference, .transaction_id = id, .source_purpose = next.source_purpose.?, .admission_digest = parsed.value.digest };
            if (prior == null) {
                if (next.status != .pending or next.prepared) return error.InvalidTxnRecord;
                try source_scope.Store.enter(scope_arena.allocator(), batch, root, slot, coordinator_root);
            } else if (next.status == .pending) {
                if (next.prepared and !prior.?.prepared) try source_scope.Store.prepareAllowed(scope_arena.allocator(), batch, root, slot);
            } else {
                if (!next.intents_resolved_known or !next.intents_resolved) return error.SourceOutcomeUnresolved;
                try source_scope.Store.finish(scope_arena.allocator(), batch, root, slot, next.status == .committed, scope_cut orelse return error.InvalidSourceAdmission);
            }
        }
        for (deletes) |key| {
            batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        for (writes) |kv| {
            try batch.put(kv.key, kv.value);
        }
        if (reservation) |change| if (change.publication) |publication| if (publication.source) |source| {
            const a = if (recording) arena.allocator() else capture_scratch.allocator();
            const namespace = @import("db/online_source_contract.zig").namespaceBytes(source.preparation.physical.namespace);
            const authority = (try @import("source_authority.zig").load(batch)) orelse return error.OnlineSourceScopeChanged;
            if (authority.kind != .raft or !std.mem.eql(u8, &namespace, &authority.namespace)) return error.OnlineSourceScopeChanged;
            if (publication.input) |outbound| {
                if (outbound.physical_root_incarnation != source.preparation.physical.physical_root_incarnation or outbound.ordered == null or
                    !std.meta.eql(outbound.ordered.?, source.position.raft)) return error.ReplicationCutoverAuthorityLost;
            }
            _ = try @import("db/replication_source_reader_floor.zig").stage(a, batch, source.catalog);
            var frozen: std.ArrayList(source_apply.FrozenIntent) = .empty;
            defer frozen.deinit(a);
            for (writes) |kv| {
                if (std.mem.startsWith(u8, kv.key, intents_prefix) and kv.key.len > intents_prefix.len + 17 and
                    std.mem.eql(u8, kv.key[intents_prefix.len..][0..16], &publication.id) and kv.key[intents_prefix.len + 16] == ':')
                    try frozen.append(a, .{ .key = kv.key[intents_prefix.len + 17 ..], .encoded_value = kv.value });
            }
            const admitted = try batch.get(&makeSidecarKey(intent_admission_prefix, publication.id));
            if (admitted.len != 32 or std.mem.readInt(u64, admitted[0..8], .little) != frozen.items.len) return error.InvalidTxnRecord;
            _ = try source_apply.stagePrepared(a, batch, source.preparation, source.position, publication.id, publication.revision, frozen.items);
        };
        if (directory) |*header| if (header.phase != .retired) {
            var changed = false;
            for (deletes) |key| if (topologyDebtTxnId(key)) |id| {
                try topology_debt.setMember(batch, header, id, try TopologyDebtClassifier.fast(batch, id));
                changed = true;
            };
            for (writes) |kv| if (topologyDebtTxnId(kv.key)) |id| {
                try topology_debt.setMember(batch, header, id, try TopologyDebtClassifier.fast(batch, id));
                changed = true;
            };
            if (changed) {
                if (header.phase == .dirty and header.pending == null and !try topology_debt.hasPrefix(batch, &topology_debt.dirtyPrefix(header.scope.generation))) {
                    header.phase = .gc;
                    header.after = null;
                }
                if (header.phase == .gc and !try topology_debt.hasPrefix(batch, &topology_debt.orphanPrefix(header.scope.generation)) and
                    !try topology_debt.hasPrefix(batch, &topology_debt.seenPrefix(header.scope.generation))) header.phase = .ready;
                try header.bump();
                try topology_debt.save(batch, header.*);
            }
        };
        if (source_retirement) |ordered| {
            const ordered_retirement = @import("source_scope_retirement_ordered.zig");
            const reply = try ordered_retirement.resolvedReply(scope_arena.allocator(), batch, ordered.command, ordered.cut, ordered.already_fenced);
            try ordered_retirement.stage(scope_arena.allocator(), batch, ordered.command, reply);
        }
        if (replay) |entry| try batch.setReplayOpaque(entry.sequence, entry.payload);
        if (recording) {
            const view = @import("commit_participant.zig").View.from(&raw_batch);
            var journal: CommitGuard.GuardedView = .{ .original = view };
            try recorder.finish(capture.sink, .{ .ptr = &journal, .get_fn = CommitGuard.GuardedView.get, .put_fn = CommitGuard.GuardedView.put });
        }
        if (reservation) |change| if (change.terminal) |sink| {
            const view = @import("commit_participant.zig").View.from(&raw_batch);
            var journal: CommitGuard.GuardedView = .{ .original = view };
            try sink.stage(sink.ptr, .{ .ptr = &journal, .get_fn = CommitGuard.GuardedView.get, .put_fn = CommitGuard.GuardedView.put });
        };
        if (reservation) |change| if (change.source_resolution) |resolution| {
            const a = if (recording) arena.allocator() else capture_scratch.allocator();
            const input = resolution.input;
            const namespace = @import("db/online_source_contract.zig").namespaceBytes(input.permit.namespace);
            _ = try @import("source_authority.zig").require(batch, .raft, namespace);
            switch (resolution.status) {
                .committed => {
                    const activated = (try source_apply.readActivated(a, batch, input.permit)) orelse return error.ReplicationCutoverAuthorityLost;
                    if (activated.image.physical_root_incarnation != input.physical_root or !input.position.strictlyAfter(activated.position))
                        return error.ReplicationCutoverAuthorityLost;
                    _ = try source_apply.stageApplied(a, batch, input.permit, input.physical_root, resolution.revision, input.position);
                },
                .aborted => _ = try source_apply.stageAborted(a, batch, input.permit, input.physical_root, input.permit.namespace, input.position),
                .pending => unreachable,
            }
        };
        if (reservation) |change| if (change.source_ack) |ack| {
            const a = if (recording) arena.allocator() else capture_scratch.allocator();
            const namespace = @import("db/online_source_contract.zig").namespaceBytes(ack.completion.permit.namespace);
            _ = try @import("source_authority.zig").require(batch, .raft, namespace);
            if (try source_apply.readAcknowledged(a, batch, ack.completion)) |existing| {
                if (existing.image.physical_root_incarnation != ack.physical_root) return error.ReplicationCutoverAuthorityLost;
            } else _ = try source_apply.stageAcknowledged(a, batch, ack.completion, ack.physical_root, ack.position);
        };
        return try raw_batch.commitStaged();
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
        const ts_key = try internal_keys.ttlKeyAlloc(self.alloc, key);
        defer self.alloc.free(ts_key);
        const val = self.getAlloc(self.alloc, ts_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(val);
        return @import("ttl.zig").writeVersion(val);
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

fn nativePinForStore(store: anytype) ?NativePhysicalPin {
    const T = @TypeOf(store);
    if (T == docstore.DocStore or T == *docstore.DocStore or T == *const docstore.DocStore) return store.native_relation_pin;
    return null;
}

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

// Local intent resolution publishes its ACK with the row effects. Keep the
// normal indexed path independent of cohort size; predecessor sidecars still
// use the compatibility merge below until their indexed migration commits.
const IndexedResolutionAck = struct {
    index_key: [participant_index_prefix.len + 16]u8 = undefined,
    member_key: [resolved_index_prefix.len + 48]u8 = undefined,
    index_value: [17]u8 = undefined,
    participant: ?[]const u8 = null,

    fn prepare(self: *@This(), manager: *TxnManager, id: TxnId, participant: []const u8) !bool {
        // Resolution is serialized by the owning apply fence. These immediate
        // point probes must not clone the LSM memtable into a bound snapshot.
        var read = try manager.store.beginProbe();
        defer read.abort();
        self.index_key = makeSidecarKey(participant_index_prefix, id);
        const raw = (try optionalTopologyValue(&read, &self.index_key)) orelse return false;
        var index = try ParticipantIndex.decode(raw);
        if (try optionalTopologyValue(&read, &makeSidecarKey(resolved_participants_prefix, id)) != null) return false;
        if (participant.len == 0) return error.InvalidParticipant;
        const member = (try optionalTopologyValue(&read, &makeParticipantIndexKey(participant_index_prefix, id, participant))) orelse return error.InvalidParticipant;
        if (!std.mem.eql(u8, member, participant)) return error.InvalidTxnRecord;
        self.member_key = makeParticipantIndexKey(resolved_index_prefix, id, participant);
        if (try optionalTopologyValue(&read, &self.member_key)) |resolved| {
            if (!std.mem.eql(u8, resolved, participant)) return error.InvalidTxnRecord;
            return true;
        }
        if (index.resolved >= index.enlisted) return error.InvalidTxnRecord;
        index.resolved += 1;
        index.encode(&self.index_value);
        self.participant = participant;
        return true;
    }

    fn append(self: *const @This(), alloc: Allocator, writes: *std.ArrayListUnmanaged(docstore.KVPair)) !void {
        if (self.participant) |participant| {
            try writes.append(alloc, .{ .key = &self.member_key, .value = participant });
            try writes.append(alloc, .{ .key = &self.index_key, .value = &self.index_value });
        }
    }
};

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

fn resolvedParticipantsInRead(alloc: Allocator, read: anytype, txn_id: TxnId) ![][]u8 {
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

fn readHasPrefix(read: anytype, prefix: []const u8) !bool {
    var cursor = try read.openCursor();
    defer cursor.close();
    const entry = (try cursor.seekAtOrAfter(prefix)) orelse return false;
    return std.mem.startsWith(u8, entry.key, prefix);
}

fn participantListCountInRead(
    read: anytype,
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

fn sameSourceScope(left: ?source_scope.Reference, right: ?source_scope.Reference) bool {
    return std.meta.eql(left, right);
}

fn samePurpose(left: ?[32]u8, right: ?[32]u8) bool {
    if (left) |value| return right != null and std.mem.eql(u8, &value, &right.?);
    return right == null;
}
fn sameBytes(left: ?[]const u8, right: ?[]const u8) bool {
    if (left) |value| return right != null and std.mem.eql(u8, value, right.?);
    return right == null;
}

/// Shared with the final history capture stage; it must inspect the staged
/// record using the actual record codec rather than duplicating byte offsets.
pub fn requireExternalCompletionRelease(raw: []const u8, owner: ExternalDecisionOwner) !void {
    const record = try decodeRecord(raw);
    const current = record.external_decision_owner orelse return error.ExternalTransactionDecisionRequired;
    if (!current.eql(owner)) return error.ExternalTransactionDecisionChanged;
    try requireCompletionResolvedRecord(raw, record.commit_version);
}

pub fn requireCompletionResolvedRecord(raw: []const u8, commit_version: u64) !void {
    const record = try decodeRecord(raw);
    if (record.status != .committed or record.commit_version != commit_version or !record.prepared_known or !record.prepared or !record.intents_resolved_known or !record.intents_resolved) return error.SourceOutcomeUnresolved;
}

/// Decode the coordinator record with its canonical codec. Only retained,
/// explicitly owned completion records qualify for restart decision recovery.
/// Order an absent decision check against late BEGIN on the original
/// checkpoint root. The caller must hold an authenticated source-attempt
/// grant for this reference and apply this as a single native transaction.
pub fn fenceUnadmittedSourceAttempt(a: Allocator, txn: anytype, root: source_scope.Basis, reference: source_scope.Reference, id: TxnId) !u64 {
    if (std.mem.allEqual(u8, &id, 0)) return error.InvalidSourceAdmission;
    const key = makeRecordKey(id);
    if (txn.get(&key)) |_| {
        return error.SourceOutcomeUnresolved;
    } else |err| switch (@as(anyerror, err)) {
        error.NotFound => {},
        else => return err,
    }
    return source_scope.Store.fenceCoordinatorUnenlisted(a, txn, root, reference);
}

/// A planned transfer may replace a fully drained ABORT. Keep the terminal
/// decision and require its exact scope plus the persisted native token fence.
pub fn fenceSettledSourceAttempt(a: Allocator, txn: anytype, root: source_scope.Basis, reference: source_scope.Reference, id: TxnId) !u64 {
    if (std.mem.allEqual(u8, &id, 0)) return error.InvalidSourceAdmission;
    const key = makeRecordKey(id);
    const raw = txn.get(&key) catch |err| switch (@as(anyerror, err)) {
        error.NotFound => return source_scope.Store.fenceCoordinatorUnenlisted(a, txn, root, reference),
        else => return err,
    };
    const record = try decodeRecord(raw);
    if (record.status != .aborted or !record.coordinator_known or !record.coordinator or
        !record.intents_resolved_known or !record.intents_resolved) return error.SourceOutcomeUnresolved;
    if (!std.meta.eql(record.source_scope, @as(?source_scope.Reference, reference))) return error.SourceTransactionIdentityConflict;
    if (try TxnManager.hasUnresolvedParticipantsInRead(a, txn, id)) return error.SourceOutcomeUnresolved;
    const scoped = try source_scope.Store.requireRecord(a, txn, root, reference.domain);
    if (!std.meta.eql(scoped.spec.checkpoint_owner, root) or scoped.closed_through_token < reference.token or
        scoped.last_committed_sequence >= reference.sequence) return error.SourceOutcomeUnresolved;
    return scoped.closed_through_token;
}

pub fn completionCoordinatorRecord(raw: []const u8) !@import("history_decision_wire.zig").Record {
    const record = try decodeRecord(raw);
    if (!record.coordinator_known or !record.coordinator or !record.retain_terminal_known or !record.prepared_known or (record.status != .pending and !record.intents_resolved_known)) return error.InvalidTxnRecord;
    const result: @import("history_decision_wire.zig").Record = .{
        .status = switch (record.status) {
            .pending => .pending,
            .committed => .committed,
            .aborted => .aborted,
        },
        .begin_timestamp = record.begin_timestamp,
        .created_at_ns = record.created_at,
        .commit_version = record.commit_version,
        .finalized_at_ns = record.finalized_at,
        .prepared = record.prepared,
        .intents_resolved = record.intents_resolved,
        .digest = completion.bytesDigest(raw),
        .retain_terminal = record.retain_terminal,
        .externally_owned = record.external_decision_owner != null,
        .source_purpose = record.source_purpose,
        .source_scope = record.source_scope,
    };
    try result.validate();
    return result;
}

/// Check the immutable membership count and acknowledged members in the
/// same snapshot as the coordinator decision. Legacy lists can safely take
/// the idempotent acknowledgment path, which upgrades them to the index.
pub fn completionAcknowledgmentsPending(read: anytype, admission: completion.Admission, retain_terminal: bool) !bool {
    const txn_id = admission.transaction_id;
    const index_key = makeSidecarKey(participant_index_prefix, txn_id);
    const raw_index = read.get(&index_key) catch |err| switch (err) {
        error.NotFound => return true,
        else => return err,
    };
    const index = try ParticipantIndex.decode(raw_index);
    if (index.enlisted != admission.participants.len) return error.InvalidTxnRecord;
    if (index.resolved == index.enlisted) return false;
    if (!retain_terminal or index.enlisted - index.resolved != 1) return true;

    const coordinator = try admission.coordinatorMember();
    const ack_key = makeParticipantIndexKey(resolved_index_prefix, txn_id, coordinator.locator);
    const ack = read.get(&ack_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, ack, coordinator.locator)) return error.InvalidTxnRecord;
    return true;
}

fn decodeRecord(raw: []const u8) !TxnRecord {
    if (raw.len == 0 or raw[0] > @backingInt(TxnStatus.aborted)) return TxnError.InvalidTxnRecord;
    if (raw.len == txn_record_external_pinned_size) {
        var record = try decodeRecord(raw[0..txn_record_external_size]);
        if (record.publication_capture_required or record.replication_source_required or record.ordinary_producer) return error.InvalidTxnRecord;
        record.native_pin = NativePhysicalPin.decode(raw[txn_record_external_size..]) catch return error.InvalidTxnRecord;
        return record;
    }
    if (raw.len == txn_record_external_size) {
        var record = try decodeRecord(raw[0..txn_record_v6_size]);
        const flags = raw[txn_record_v6_size];
        if (raw[txn_record_external_size - 1] > 1 or flags & ~@as(u8, 29) != 0) return error.InvalidTxnRecord;
        record.document_revisions = flags & 1 != 0;
        record.publication_capture_required = flags & 4 != 0;
        record.replication_source_required = flags & 8 != 0;
        record.ordinary_producer = flags & 16 != 0;
        if (record.ordinary_producer and (record.replication_source_required or !record.retain_terminal))
            return error.InvalidTxnRecord;
        if (!record.prepared and (record.publication_capture_required or (record.replication_source_required and record.status != .aborted)))
            return error.InvalidTxnRecord;
        record.external_decision_owner = ExternalDecisionOwner.decode(raw[txn_record_v7_size..][0..ExternalDecisionOwner.encoded_len]) catch return error.InvalidTxnRecord;
        record.external_commit_started = raw[txn_record_external_size - 1] == 1;
        if ((record.external_commit_started and !record.prepared) or
            (record.status == .committed and !record.external_commit_started) or
            (record.status == .aborted and record.external_commit_started)) return error.InvalidTxnRecord;
        return record;
    }
    if (raw.len == txn_record_v10_size) {
        if (raw[53] > 1 or (raw[54] != 1 and raw[54] != 2)) return error.InvalidTxnRecord;
        var record = try decodeRecord(raw[0..txn_record_v6_size]);
        record.ordinary_producer = raw[54] == 1;
        record.legacy_producer = raw[54] == 2;
        record.document_revisions = raw[53] == 1;
        return record;
    }
    if (raw.len == txn_record_v9_size) {
        if (raw[53] > 1 or raw[54] != 2 or std.mem.allEqual(u8, raw[55..87], 0)) return error.InvalidTxnRecord;
        var record = try decodeRecord(raw[0..txn_record_v6_size]);
        if (!record.retain_terminal) return error.InvalidTxnRecord;
        record.document_revisions = raw[53] == 1;
        record.source_purpose = raw[55..87].*;
        record.source_scope = source_scope.Reference.decode(raw[87..]) catch return error.InvalidTxnRecord;
        return record;
    }
    if (raw.len == txn_record_v8_size) {
        if (raw[53] > 1 or raw[54] != 1 or std.mem.allEqual(u8, raw[55..87], 0)) return error.InvalidTxnRecord;
        var record = try decodeRecord(raw[0..txn_record_v6_size]);
        record.document_revisions = raw[53] == 1;
        record.source_purpose = raw[55..87].*;
        return record;
    }
    if (raw.len == txn_record_v7_size) {
        if (raw[txn_record_v6_size] != 1) return error.InvalidTxnRecord;
        var record = try decodeRecord(raw[0..txn_record_v6_size]);
        record.document_revisions = true;
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
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn encodeParticipantList(alloc: Allocator, participants: []const []const u8) ![]u8 {
    if (participants.len > std.math.maxInt(u32)) return error.TransactionTooLarge;
    var total: usize = 4;
    for (participants) |participant| {
        if (participant.len > std.math.maxInt(u32)) return error.TransactionTooLarge;
        total = std.math.add(usize, total, std.math.add(usize, 4, participant.len) catch return error.TransactionTooLarge) catch return error.TransactionTooLarge;
    }
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
    return try alloc.dupeSentinel(u8, path, 0);
}

// ============================================================================
// Tests
// ============================================================================

test "committed history completion admission freezes the coordinator plan with begin and survives reopen" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "completion-admission");
    defer a.free(path);
    defer cleanupTestDir(path);
    const namespace: completion.Namespace = @splat(1);
    const members = [_]completion.Participant{
        .{ .namespace = namespace, .owner_fence = @splat(2), .roles = completion.Roles.effects, .locator = "original-writer" },
        .{ .namespace = @splat(3), .owner_fence = @splat(4), .roles = completion.Roles.guards, .locator = "original-guard" },
    };
    const admission: completion.Admission = .{ .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = namespace, .coordinator_fence = @splat(2), .participants = &members };
    const marker_key = "\x00\x00__completion_test_marker";
    var receipt_digest: completion.Digest = @splat(0);
    {
        var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        {
            const history = @import("committed_history.zig");
            var bind = try store.beginWriteTxn();
            errdefer bind.abort();
            try bind.put(&internal_keys.identity_namespace_key, &namespace);
            try @import("source_authority.zig").bind(&bind, .native, namespace);
            try bind.commit();
            var enroll = try store.beginWriteTxn();
            errdefer enroll.abort();
            const consumer = try history.admit(&enroll, namespace, @splat(1), 4 * 1024 * 1024, 1000, 1);
            try enroll.commit();
            var release = try store.beginWriteTxn();
            errdefer release.abort();
            try history.release(&release, namespace, consumer.id, consumer.epoch);
            try release.commit();
        }
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{ .writes = &.{.{ .key = marker_key, .value = "first-begin" }} });
        {
            var enrollment = try store.beginWriteTxn();
            defer enrollment.abort();
            const history = @import("committed_history.zig");
            try std.testing.expectError(error.HistoryPreparedTransactionsPresent, history.admitAtWithCompletionProof(&enrollment, namespace, @splat(9), 4 * 1024 * 1024, 1000, 1, null, true));
        }
        try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{ .writes = &.{.{ .key = marker_key, .value = "confirmed-retry" }} });
        const plan_key = makeSidecarKey(completion_admission_prefix, admission.transaction_id);
        const plan_bytes = try manager.getAlloc(a, &plan_key);
        defer a.free(plan_bytes);
        const original_encoding = try std.fmt.allocPrint(a, "{s}\n ", .{plan_bytes});
        defer a.free(original_encoding);
        try store.put(&plan_key, original_encoding);
    }
    {
        var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        var saved = (try manager.getCompletionAdmission(a, admission.transaction_id)).?;
        defer saved.deinit();
        try std.testing.expectEqual(admission.digest(), saved.value.digest);
        try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{});
        const plan_key = makeSidecarKey(completion_admission_prefix, admission.transaction_id);
        const original_encoding = try manager.getAlloc(a, &plan_key);
        defer a.free(original_encoding);
        try std.testing.expect(std.mem.endsWith(u8, original_encoding, "\n "));
        const marker = try manager.getAlloc(a, marker_key);
        defer a.free(marker);
        try std.testing.expectEqualStrings("confirmed-retry", marker);
        const record = try manager.loadTransactionRecord(admission.transaction_id);
        try std.testing.expect(record.coordinator and record.retain_terminal);
        try std.testing.expectEqual(@as(u64, 10), record.begin_timestamp);
        const plan_members = try manager.getParticipants(a, admission.transaction_id);
        defer freeParticipantList(a, plan_members);
        try std.testing.expectEqual(@as(usize, 2), plan_members.len);
        try std.testing.expectError(TxnError.DecisionConflict, manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(admission.transaction_id, 10, 11, &.{ "original-writer", "original-guard" }, true, true));
        var changed = admission;
        changed.admission_digest[0] ^= 1;
        try std.testing.expectError(TxnError.DecisionConflict, manager.initTransactionWithCompletionAdmission(changed, 10, 11, true, .{}));
        changed = admission;
        changed.coordinator_namespace = members[1].namespace;
        changed.coordinator_fence = members[1].owner_fence;
        try std.testing.expectError(TxnError.DecisionConflict, manager.initTransactionWithCompletionAdmission(changed, 10, 11, true, .{}));
        const legacy_id: TxnId = @splat(8);
        try manager.initTransaction(legacy_id, 10);
        changed = admission;
        changed.transaction_id = legacy_id;
        try std.testing.expectError(TxnError.DecisionConflict, manager.initTransactionWithCompletionAdmission(changed, 10, 11, true, .{}));
        try std.testing.expectError(error.SourceOutcomeUnresolved, manager.resolveIntents(admission.transaction_id, .committed, 20));
        try manager.writeIntents(admission.transaction_id, &.{}, &.{});
        try manager.resolveIntents(admission.transaction_id, .committed, 20);
        {
            var receipt = (try manager.getCompletionResolution(a, admission.transaction_id)).?;
            defer receipt.deinit();
            try std.testing.expectEqual(std.meta.Tag(completion.Outcome).no_effect, std.meta.activeTag(receipt.value.resolution.outcome));
            try std.testing.expectEqual(@as(u8, 2), receipt.value.resolution.source_position[31]);
            receipt_digest = receipt.value.digest;
        }
        try manager.resolveIntents(admission.transaction_id, .committed, 99);
        try std.testing.expectError(error.NotFound, manager.getAlloc(a, &completion.requestKey(admission.transaction_id)));
        try manager.markParticipantsResolvedExtraBatch(admission.transaction_id, &.{ "original-writer", "original-guard" }, .{});
        try std.testing.expect(!(try manager.cleanupTransactionMetadataIfEligible(admission.transaction_id, std.math.maxInt(u64), std.math.maxInt(u64))));
        var retained = (try manager.getCompletionAdmission(a, admission.transaction_id)).?;
        defer retained.deinit();
        try std.testing.expectEqual(admission.digest(), retained.value.digest);
        const committed_manifest = try retained.value.admission.manifest(20);
        try std.testing.expectEqual(@as(u64, 20), committed_manifest.commit_version);
        try std.testing.expectError(error.InvalidTxnRecord, retained.value.admission.manifest(0));
        var aborted = admission;
        aborted.transaction_id = @splat(9);
        try manager.initTransactionWithCompletionAdmission(aborted, 30, 31, true, .{});
        try manager.resolveIntents(aborted.transaction_id, .aborted, 0);
        try manager.markParticipantsResolvedExtraBatch(aborted.transaction_id, &.{ "original-writer", "original-guard" }, .{});
        try std.testing.expect(try manager.cleanupTransactionMetadataIfEligible(aborted.transaction_id, std.math.maxInt(u64), std.math.maxInt(u64)));
        try std.testing.expect((try manager.getCompletionAdmission(a, aborted.transaction_id)) == null);
    }
    {
        var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        var receipt = (try manager.getCompletionResolution(a, admission.transaction_id)).?;
        defer receipt.deinit();
        try std.testing.expectEqual(receipt_digest, receipt.value.digest);
        try std.testing.expectEqual(@as(u64, 20), receipt.value.resolution.commit_version);
        {
            const wire = @import("history_resolution_wire.zig");
            var shadow_gc = try store.beginWriteTxn();
            defer shadow_gc.abort();
            try shadow_gc.delete(&makeRecordKey(admission.transaction_id));
            try shadow_gc.delete(&(completion.admission_prefix.* ++ admission.transaction_id));
            const current = (try @import("source_authority.zig").load(&shadow_gc)).?;
            const request: wire.Request = .{ .authority = .native, .namespace = namespace, .transaction_id = admission.transaction_id, .owner_fence = admission.coordinator_fence, .manifest_digest = receipt.value.resolution.manifest_digest, .commit_version = 20 };
            const historical = try wire.read(a, &shadow_gc, request, .{ .native_sequence = current.sequence });
            try std.testing.expectEqualDeep(receipt.value.resolution, historical.resolution.?);
        }
    }
}

test "committed history completion admission credits survive resolution and reject full begin atomically" {
    const history = @import("committed_history.zig");
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "completion-credits");
    defer a.free(path);
    defer cleanupTestDir(path);
    const namespace: completion.Namespace = @splat(17);
    const members = [_]completion.Participant{
        .{ .namespace = namespace, .owner_fence = @splat(2), .roles = completion.Roles.effects, .locator = "writer" },
        .{ .namespace = @splat(19), .owner_fence = @splat(4), .roles = completion.Roles.guards, .locator = "guard" },
    };
    const admission: completion.Admission = .{ .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = namespace, .coordinator_fence = @splat(2), .participants = &members };
    const encoded = try completion.encodeAdmission(a, admission);
    defer a.free(encoded);
    const credit = try admission.reservationBytes(encoded.len);
    var receipt_digest: completion.Digest = @splat(0);
    const State = struct {
        fn read(store: *DocStore) !history.State {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            return (try history.load(&txn)).?;
        }
    };
    for (0..2) |phase| {
        var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        if (phase == 0) {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(&internal_keys.identity_namespace_key, &namespace);
            try @import("source_authority.zig").bind(&txn, .native, namespace);
            try txn.commit();
            var enrollment = try store.beginWriteTxn();
            errdefer enrollment.abort();
            const consumer = try history.admit(&enrollment, namespace, @splat(1), 4 * 1024 * 1024, 1000, 1);
            try enrollment.commit();
            try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{});
            try std.testing.expectEqual(credit, (try State.read(&store)).reserved_bytes);
            try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{});
            try std.testing.expectEqual(credit, (try State.read(&store)).reserved_bytes);
            try manager.writeIntents(admission.transaction_id, &.{.{ .key = "row", .value = "after" }}, &.{});
            try std.testing.expect((try State.read(&store)).reserved_bytes > credit);
            const prepared = try State.read(&store);
            const record_key = makeRecordKey(admission.transaction_id);
            try std.testing.expectError(error.InvalidTxnRecord, manager.resolveIntentsWithExtraBatch(admission.transaction_id, .committed, 20, .{ .completion_writes = &.{.{ .key = &record_key, .value = "invalid-staged-record" }} }));
            try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(admission.transaction_id));
            try std.testing.expectEqualDeep(prepared, try State.read(&store));
            const primary = try internal_keys.documentKeyAlloc(a, "row");
            defer a.free(primary);
            try std.testing.expectError(error.NotFound, manager.getAlloc(a, primary));
            try std.testing.expectError(error.NotFound, manager.getAlloc(a, &history.recordKey(1)));
            try std.testing.expectError(error.NotFound, manager.getAlloc(a, &completion.resolutionKey(admission.transaction_id)));
            try manager.resolveIntents(admission.transaction_id, .committed, 20);
            {
                var receipt = (try manager.getCompletionResolution(a, admission.transaction_id)).?;
                defer receipt.deinit();
                const effects = receipt.value.resolution.outcome.effects;
                const descriptor = try manager.getAlloc(a, &history.recordKey(effects.sequence));
                defer a.free(descriptor);
                try std.testing.expectEqual(completion.bytesDigest(descriptor), effects.frame_digest);
                try std.testing.expectEqual(@as(u64, 1), effects.sequence);
                receipt_digest = receipt.value.digest;
            }
            try std.testing.expectEqual(credit, (try State.read(&store)).reserved_bytes);
            var release = try store.beginWriteTxn();
            errdefer release.abort();
            try history.release(&release, namespace, consumer.id, consumer.epoch);
            try release.commit();
            try std.testing.expectEqual(credit, (try State.read(&store)).reserved_bytes);
        } else {
            try std.testing.expectEqual(credit, (try State.read(&store)).reserved_bytes);
            {
                var late = try store.beginWriteTxn();
                defer late.abort();
                try std.testing.expectError(error.HistoryPreparedTransactionsPresent, history.admit(&late, namespace, @splat(2), 4 * 1024 * 1024, 1000, 1));
            }
            var aborted = admission;
            aborted.transaction_id = @splat(8);
            try manager.initTransactionWithCompletionAdmission(aborted, 30, 31, true, .{});
            try std.testing.expect((try State.read(&store)).reserved_bytes > credit);
            try manager.resolveIntents(aborted.transaction_id, .aborted, 0);
            try std.testing.expect(!(try manager.cleanupTransactionMetadataIfEligible(aborted.transaction_id, 0, 0)));
            {
                var unresolved_plan = (try manager.getCompletionAdmission(a, aborted.transaction_id)).?;
                defer unresolved_plan.deinit();
                try std.testing.expectEqual(aborted.digest(), unresolved_plan.value.digest);
            }
            try std.testing.expect((try State.read(&store)).reserved_bytes > credit);
            try manager.markParticipantsResolvedExtraBatch(aborted.transaction_id, &.{ "writer", "guard" }, .{});
            try std.testing.expect(!(try manager.cleanupTransactionMetadataIfEligible(aborted.transaction_id, 0, 0)));
            try std.testing.expect((try manager.getCompletionAdmission(a, aborted.transaction_id)) == null);
            try std.testing.expectEqual(TxnStatus.aborted, (try manager.loadTransactionRecord(aborted.transaction_id)).status);
            try std.testing.expectEqual(credit, (try State.read(&store)).reserved_bytes);
            try std.testing.expect(try manager.cleanupTransactionMetadataIfEligible(aborted.transaction_id, std.math.maxInt(u64), std.math.maxInt(u64)));
            try std.testing.expectEqual(credit, (try State.read(&store)).reserved_bytes);
            // With no live consumer, acknowledged frames can be reclaimed
            // inline. Exhaust uncollectable admission credits to prove the
            // failed BEGIN rolls back its companion marker.
            var rejected = admission;
            const attempt_limit = (try State.read(&store)).limit / 4096 + 1;
            for (0..attempt_limit) |candidate| {
                rejected.transaction_id = @splat(7);
                std.mem.writeInt(u64, rejected.transaction_id[0..8], candidate, .little);
                const candidate_bytes = try completion.encodeAdmission(a, rejected);
                defer a.free(candidate_bytes);
                const state = try State.read(&store);
                if (try rejected.reservationBytes(candidate_bytes.len) > state.limit - state.reserved_bytes) break;
                try manager.initTransactionWithCompletionAdmission(rejected, 40, 41, true, .{});
            } else return error.TestExpectedError;
            const full = try State.read(&store);
            const marker = "\x00\x00__completion_quota_test_marker";
            try std.testing.expectError(error.HistoryFull, manager.initTransactionWithCompletionAdmission(rejected, 40, 41, true, .{ .writes = &.{.{ .key = marker, .value = "must-not-commit" }} }));
            try std.testing.expect((try manager.getCompletionAdmission(a, rejected.transaction_id)) == null);
            try std.testing.expectError(TxnError.TxnNotFound, manager.loadTransactionRecord(rejected.transaction_id));
            try std.testing.expectError(error.NotFound, manager.getAlloc(a, marker));
            try std.testing.expectEqualDeep(full, try State.read(&store));
            var receipt = (try manager.getCompletionResolution(a, admission.transaction_id)).?;
            defer receipt.deinit();
            try std.testing.expectEqual(receipt_digest, receipt.value.digest);
        }
    }
}

test "committed history completion admission binds guard participant votes and original recovery owner" {
    const history = @import("committed_history.zig");
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "completion-participant");
    defer a.free(path);
    defer cleanupTestDir(path);
    const namespace: completion.Namespace = @splat(20);
    const members = [_]completion.Participant{
        .{ .namespace = namespace, .owner_fence = @splat(2), .roles = completion.Roles.guards, .locator = "guard" },
        .{ .namespace = @splat(40), .owner_fence = @splat(4), .roles = completion.Roles.checkpoint, .locator = "coordinator" },
    };
    const admission: completion.Admission = .{ .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = @splat(40), .coordinator_fence = @splat(4), .participants = &members };
    var credit: u64 = 0;
    for (0..2) |phase| {
        var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        if (phase == 0) {
            var bind = try store.beginWriteTxn();
            errdefer bind.abort();
            try bind.put(&internal_keys.identity_namespace_key, &namespace);
            try @import("source_authority.zig").bind(&bind, .native, namespace);
            try bind.commit();
            try std.testing.expectError(error.HistoryResynchronizationRequired, manager.initParticipantWithCompletionAdmission(admission, 10, 11, true, .{}));
            try std.testing.expectError(TxnError.TxnNotFound, manager.loadTransactionRecord(admission.transaction_id));
            var enroll = try store.beginWriteTxn();
            errdefer enroll.abort();
            const consumer = try history.admit(&enroll, namespace, @splat(1), 4 * 1024 * 1024, 1000, 1);
            try enroll.commit();
            var release = try store.beginWriteTxn();
            errdefer release.abort();
            try history.release(&release, namespace, consumer.id, consumer.epoch);
            try release.commit();
            try manager.initParticipantWithCompletionAdmission(admission, 10, 11, true, .{});
            try std.testing.expect(!(try manager.loadTransactionRecord(admission.transaction_id)).coordinator);
            try std.testing.expectError(error.SourceOutcomeUnresolved, manager.resolveIntents(admission.transaction_id, .committed, 20));
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                const state = (try history.load(&read)).?;
                try std.testing.expect(!state.active());
                credit = state.reserved_bytes;
                try std.testing.expect(credit > 0);
            }
            try std.testing.expectError(error.InvalidParticipant, manager.writeIntents(admission.transaction_id, &.{.{ .key = "row", .value = "must-not-write" }}, &.{}));
            try std.testing.expect(!(try manager.loadTransactionRecord(admission.transaction_id)).prepared);
            try manager.writeIntents(admission.transaction_id, &.{}, &.{.{ .key = "row", .expected_version = 0 }});
        } else {
            var plan = (try manager.getCompletionAdmission(a, admission.transaction_id)).?;
            defer plan.deinit();
            try std.testing.expectEqual(admission.digest(), plan.value.digest);
            const owners = try manager.getParticipants(a, admission.transaction_id);
            defer freeParticipantList(a, owners);
            try std.testing.expectEqual(@as(usize, 1), owners.len);
            try std.testing.expectEqualStrings("coordinator", owners[0]);
            const record = try manager.loadTransactionRecord(admission.transaction_id);
            try std.testing.expect(record.prepared and !record.coordinator);
            _ = try manager.recoverTransactions(1000, 2000);
            try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(admission.transaction_id));
            try manager.resolveIntents(admission.transaction_id, .committed, 20);
            var receipt = (try manager.getCompletionResolution(a, admission.transaction_id)).?;
            defer receipt.deinit();
            try std.testing.expectEqual(std.meta.Tag(completion.Outcome).no_effect, std.meta.activeTag(receipt.value.resolution.outcome));
            try std.testing.expectEqual(namespace, receipt.value.resolution.namespace);
            try std.testing.expect(std.mem.readInt(u64, receipt.value.resolution.source_position[0..8], .little) > 0);
            const receipt_key = completion.resolutionKey(admission.transaction_id);
            const original = try manager.getAlloc(a, &receipt_key);
            defer a.free(original);
            const manifest = try admission.manifest(20);
            var foreign = receipt.value.resolution;
            foreign.namespace = admission.coordinator_namespace;
            foreign.owner_fence = admission.coordinator_fence;
            foreign.outcome = .{ .no_effect = completion.noEffectProof(manifest, foreign.namespace, foreign.source_position) };
            const encoded_foreign = try completion.encodeResolution(a, manifest, foreign);
            defer a.free(encoded_foreign);
            try store.put(&receipt_key, encoded_foreign);
            try std.testing.expectError(error.InvalidTxnRecord, manager.getCompletionResolution(a, admission.transaction_id));
            try store.put(&receipt_key, original);
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(credit, (try history.load(&read)).?.reserved_bytes);
            try std.testing.expectEqual(@as(u64, 0), (try history.load(&read)).?.latest);
        }
    }
    const coordinator_path = try tempTestPath(a, "completion-original-owner");
    defer a.free(coordinator_path);
    defer cleanupTestDir(coordinator_path);
    var backend = try lsm_backend.Backend.open(a, coordinator_path, .{ .flush_threshold_bytes = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try store.put(&internal_keys.identity_namespace_key, &admission.coordinator_namespace);
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{});
    const recovery = try manager.getParticipants(a, admission.transaction_id);
    defer freeParticipantList(a, recovery);
    try std.testing.expectEqualStrings("coordinator", recovery[0]);
    try std.testing.expectEqualStrings("guard", recovery[1]);
}

test "committed history completion admission raft resolution requires applying cut and preserves original receipt" {
    const history = @import("committed_history.zig");
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const namespace: completion.Namespace = @splat(33);
    {
        var bind = try store.beginWriteTxn();
        errdefer bind.abort();
        try bind.put(&internal_keys.identity_namespace_key, &namespace);
        try @import("source_authority.zig").bind(&bind, .raft, namespace);
        try bind.commit();
        var enroll = try store.beginWriteTxn();
        errdefer enroll.abort();
        _ = try history.admit(&enroll, namespace, @splat(1), 4 * 1024 * 1024, 1000, 1);
        try enroll.commit();
    }
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 3, .topology_epoch = 4 };
    const input_digest = completion.preparedInputDigest(namespace, "guard-only-input");
    const members = [_]completion.Participant{.{ .namespace = namespace, .owner_fence = owner.digest(), .roles = completion.Roles.guards, .locator = "coordinator", .input_digest = input_digest }};
    const admission: completion.Admission = .{ .version = 2, .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = namespace, .coordinator_fence = owner.digest(), .participants = &members };
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{});
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.writeIntents(admission.transaction_id, &.{}, &.{}));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.writeIntentsExtraBatch(admission.transaction_id, &.{}, &.{}, .{ .completion_input_digest = @splat(7), .completion_owner = owner }));
    try std.testing.expectEqual(@as(?bool, false), try manager.completionPrepareStatus(admission.transaction_id, input_digest, null, owner));
    try manager.writeIntentsExtraBatch(admission.transaction_id, &.{}, &.{}, .{ .completion_input_digest = input_digest, .completion_owner = owner });
    try std.testing.expectEqual(@as(?bool, true), try manager.completionPrepareStatus(admission.transaction_id, input_digest, null, owner));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.completionPrepareStatus(admission.transaction_id, input_digest, @splat(8), owner));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.completionPrepareStatus(admission.transaction_id, null, null, owner));
    try std.testing.expectError(error.HistoryAuthorityRequired, manager.resolveIntents(admission.transaction_id, .committed, 20));
    try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(admission.transaction_id));
    try std.testing.expectError(error.NotFound, manager.getAlloc(a, &completion.requestKey(admission.transaction_id)));
    try std.testing.expectError(error.SourceOutcomeUnresolved, manager.getCompletionResolution(a, admission.transaction_id));
    var marker: [16]u8 = @splat(0);
    std.mem.writeInt(u64, marker[0..8], 2, .little);
    std.mem.writeInt(u64, marker[8..16], 3, .little);
    _ = try manager.resolveIntentsWithExtraBatch(admission.transaction_id, .committed, 20, .{ .writes = &.{.{ .key = &internal_keys.ordered_document_applied_entry_key, .value = &marker }} });
    var first = (try manager.getCompletionResolution(a, admission.transaction_id)).?;
    defer first.deinit();
    try std.testing.expectEqual(std.meta.Tag(completion.Outcome).no_effect, std.meta.activeTag(first.value.resolution.outcome));
    try std.testing.expectEqual(@as(u8, 1), first.value.resolution.source_position[31]);
    try std.testing.expectEqualSlices(u8, &marker, first.value.resolution.source_position[0..16]);
    std.mem.writeInt(u64, marker[8..16], 4, .little);
    _ = try manager.resolveIntentsWithExtraBatch(admission.transaction_id, .committed, 99, .{ .completion_writes = &.{.{ .key = &internal_keys.ordered_document_applied_entry_key, .value = &marker }} });
    var replayed = (try manager.getCompletionResolution(a, admission.transaction_id)).?;
    defer replayed.deinit();
    try std.testing.expectEqual(first.value.digest, replayed.value.digest);
    const applied = try manager.getAlloc(a, &internal_keys.ordered_document_applied_entry_key);
    defer a.free(applied);
    try std.testing.expectEqualSlices(u8, &marker, applied);
    try std.testing.expectEqual(@as(u64, 20), replayed.value.resolution.commit_version);
}

test "committed history completion admission keeps external certificates through the consumer floor" {
    const history = @import("committed_history.zig");
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-external-completion-release");
    defer a.free(path);
    defer cleanupTestDir(path);
    var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const namespace: completion.Namespace = @splat(33);
    const consumer_id: [16]u8 = @splat(1);
    const consumer = enrolled: {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(&internal_keys.identity_namespace_key, &namespace);
        try @import("source_authority.zig").bind(&txn, .raft, namespace);
        const enrollment = try history.admit(&txn, namespace, consumer_id, history.default_limit, 1000, 1);
        try txn.commit();
        break :enrolled enrollment;
    };
    const owner: completion.RoutedOwner = .{
        .metadata_group_id = 1,
        .metadata_incarnation = @splat('1'),
        .namespace = namespace,
        .table_binding = @splat(2),
        .root_generation = 0,
        .group_id = 3,
        .topology_epoch = 4,
    };
    const external: ExternalDecisionOwner = .{ .metadata_incarnation = owner.metadata_incarnation, .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const id: TxnId = @splat(4);
    const input = completion.preparedInputDigest(namespace, "effects");
    const members = [_]completion.Participant{.{
        .namespace = namespace,
        .owner_fence = owner.digest(),
        .roles = completion.Roles.effects,
        .locator = "table2:00000004:docs:3",
        .input_digest = input,
        .routed_owner = owner,
    }};
    const admission: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(6), .coordinator_namespace = namespace, .coordinator_fence = owner.digest(), .participants = &members };
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{ .ordinary_producer = true, .completion_owner = owner, .external_decision_owner = external });
    try manager.writeIntentsExtraBatch(id, &.{.{ .key = "row", .value = "{}" }}, &.{}, .{
        .ordinary_producer = true,
        .completion_owner = owner,
        .completion_input_digest = input,
        .external_decision_owner = external,
    });
    try manager.activateExternalDecision(id, external, .{});
    var marker: [16]u8 = @splat(0);
    std.mem.writeInt(u64, marker[0..8], 2, .little);
    std.mem.writeInt(u64, marker[8..16], 3, .little);
    _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 20, .{
        .completion_owner = owner,
        .external_decision_owner = external,
        .writes = &.{.{ .key = &internal_keys.ordered_document_applied_entry_key, .value = &marker }},
    });
    var receipt = (try manager.getCompletionResolution(a, id)).?;
    defer receipt.deinit();
    try std.testing.expectEqual(@as(u64, 1), receipt.value.resolution.outcome.effects.sequence);
    const raw = try completion.encode(a, .{ .manifest = try admission.manifest(20), .resolutions = &.{receipt.value.resolution} });
    defer a.free(raw);
    const published = observed: {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const publication = try history.publishCompletion(a, &txn, namespace, 0, owner, raw, .{ .term = 2, .index = 4 }, true);
        std.mem.writeInt(u64, marker[8..16], 4, .little);
        try txn.put(&internal_keys.ordered_document_applied_entry_key, &marker);
        try txn.commit();
        break :observed publication;
    };
    const custody: @import("db/types.zig").CompletionRelease = .{ .decision_owner = external, .certificate_digest = published.digest };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var wrong = custody;
        wrong.decision_owner.admission_digest[0] ^= 1;
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, history.releaseCompletion(a, &txn, namespace, owner, id, wrong));
        try history.releaseCompletion(a, &txn, namespace, owner, id, custody);
        try history.releaseCompletion(a, &txn, namespace, owner, id, custody);
        try std.testing.expect(try history.reclaim(&txn, namespace));
        try std.testing.expect((try history.load(&txn)).?.retained_bytes > published.retained_bytes);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try history.acknowledge(&txn, namespace, consumer_id, consumer.epoch, 0, 1);
        try std.testing.expect(try history.reclaim(&txn, namespace));
        try history.releaseCompletion(a, &txn, namespace, owner, id, custody);
        try std.testing.expectEqual(@as(u64, 0), (try history.load(&txn)).?.retained_bytes);
        try std.testing.expectError(error.NotFound, txn.get(&completion.resolutionKey(id)));
        try txn.commit();
    }
}

test "committed history transaction reservations survive reopen and committed recovery emits one original transaction" {
    const history = @import("committed_history.zig");
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-committed-history");
    defer alloc.free(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(77);
    const namespace: history.frame.Namespace = @splat(6);
    const physical = try internal_keys.documentKeyAlloc(alloc, "row");
    defer alloc.free(physical);
    var consumer: history.Consumer = undefined;
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(&internal_keys.identity_namespace_key, &namespace);
            try @import("source_authority.zig").bind(&txn, .native, namespace);
            try txn.commit();
        }
        try store.put(physical, "before");
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            consumer = try history.admit(&txn, namespace, @splat(1), history.default_limit, 1000, 1);
            try txn.commit();
        }
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.initTransaction(id, 1);
        try manager.writeIntents(id, &.{.{ .key = "row", .value = "after" }}, &.{});
        try manager.writeIntents(id, &.{ .{ .key = "row", .value = "replacement" }, .{ .key = "row", .value = "after" } }, &.{});
        var read = try store.beginReadTxn();
        defer read.abort();
        const state = (try history.load(&read)).?;
        try std.testing.expect(state.reserved_bytes > 2 * 1024 * 1024);
        try std.testing.expectEqual(@as(u64, 0), state.latest);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var manager = try TxnManager.init(alloc, &store);
        defer manager.deinit();
        try manager.resolveIntents(id, .committed, 1234);
        try manager.resolveIntents(id, .committed, 1234);
        var read = try store.beginReadTxn();
        defer read.abort();
        const state = (try history.load(&read)).?;
        try std.testing.expectEqual(@as(u64, 0), state.reserved_bytes);
        try std.testing.expectEqual(@as(u64, 1), state.latest);
        try std.testing.expectEqualStrings("after", try read.get(physical));
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const descriptor = (try history.read(scratch, &read, namespace, consumer.id, consumer.epoch, 0)).?;
        var source = try history.ChunkSource(DocStore.Txn).init(alloc, &read, descriptor);
        defer source.deinit();
        var reader: history.frame.Reader = .{ .descriptor = descriptor, .source = source.source() };
        const header = try reader.header(scratch);
        try std.testing.expectEqual(history.frame.TransactionScope.coordinated, header.value.transaction_scope);
        try std.testing.expectEqualSlices(u8, &id, &header.value.transaction_id);
        const effect = (try reader.next(scratch)).?;
        try std.testing.expectEqualStrings("before", effect.before.?);
        try std.testing.expectEqualStrings("after", effect.after.?);
        try std.testing.expect((try reader.next(scratch)) == null);
        const later_id: TxnId = @splat(78);
        try manager.initTransaction(later_id, 2);
        try manager.writeIntents(later_id, &.{.{ .key = "row", .value = "after-release" }}, &.{});
        {
            var control = try store.beginWriteTxn();
            errdefer control.abort();
            try history.release(&control, namespace, consumer.id, consumer.epoch);
            try control.commit();
        }
        try manager.resolveIntents(later_id, .committed, 1235);
        var current = try store.beginReadTxn();
        defer current.abort();
        const final_state = (try history.load(&current)).?;
        try std.testing.expect(!final_state.active());
        try std.testing.expectEqual(@as(u64, 0), final_state.reserved_bytes);
        try std.testing.expectEqual(@as(u64, 2), final_state.latest);
        try std.testing.expectEqualStrings("after-release", try current.get(physical));
    }
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
    const integrity = @import("db/relational/integrity/contract.zig");
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
    const address = try @import("db/relational/integrity/contract.zig").Address.init(@splat(1), "parent");
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
        .prepared_graph_effects_checked = true,
        .writes = &.{.{ .key = "resolved", .value = "ok" }},
    });
    try std.testing.expect(!try mgr.hasSchemaLeases());
    try std.testing.expect(!try mgr.hasIntents(txn_id));
}

test "transaction source purpose fences sessions freezes prepared intents and survives recovery" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-source-purpose");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var mgr = try TxnManager.init(a, &store);
    defer mgr.deinit();
    const txn_id: TxnId = @splat(12);
    const proof: [32]u8 = @splat(7);
    const extra: MutationExtraBatch = .{ .source_purpose = proof };
    try mgr.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(txn_id, 10_000, 1_000, &.{"docs:0"}, true, true, extra);
    try mgr.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(txn_id, 10_000, 9_000, &.{"docs:0"}, true, true, extra);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.initTransactionWithParticipantsCreatedAtRoleAndRetention(txn_id, 10_000, 1_000, &.{"docs:0"}, true, true));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(txn_id, 10_000, 1_000, &.{"docs:0"}, true, true, .{ .source_purpose = @splat(8) }));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.writeIntents(txn_id, &.{.{ .key = "a", .value = "session" }}, &.{}));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.resolveIntents(txn_id, .aborted, 11_000));
    try std.testing.expectError(error.SourceOutcomeUnresolved, mgr.resolveIntentsWithExtraBatch(txn_id, .committed, 11_000, .{ .source_purpose = proof }));
    const effects = [_]WriteIntent{ .{ .key = "a", .value = "source" }, .{ .key = "b", .value = null } };
    try mgr.writeIntentsExtraBatch(txn_id, &effects, &.{}, extra);
    var first = try mgr.collectIntentBatch(a, txn_id);
    defer first.deinit(a);
    // Equivalent final intent sets may arrive in another order. The original
    // durable vote, schema lease and reservation revision remain unchanged.
    try mgr.writeIntentsExtraBatch(txn_id, &.{ effects[1], effects[0] }, &.{}, extra);
    try std.testing.expectError(error.SourcePayloadConflict, mgr.writeIntentsExtraBatch(txn_id, &.{effects[0]}, &.{}, extra));
    try std.testing.expectError(error.SourcePayloadConflict, mgr.writeIntentsExtraBatch(txn_id, &.{ effects[0], .{ .key = "b", .value = "changed" } }, &.{}, extra));
    try std.testing.expectError(error.SourcePayloadConflict, mgr.writeIntentsExtraBatch(txn_id, &.{ effects[0], effects[1], .{ .key = "c", .value = "added" } }, &.{}, extra));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.writeIntents(txn_id, &effects, &.{}));
    var unchanged = try mgr.collectIntentBatch(a, txn_id);
    defer unchanged.deinit(a);
    try std.testing.expectEqual(first.revision, unchanged.revision);
    const stats = try mgr.recoverTransactions(20_000, 30_000);
    try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
    try std.testing.expectEqual(@as(u64, 1), stats.kept_recent_pending);
    const summaries = try mgr.listTransactions(a);
    defer a.free(summaries);
    try std.testing.expectEqual(proof, summaries[0].source_purpose.?);
    const raw = try mgr.getAlloc(a, &makeRecordKey(txn_id));
    defer a.free(raw);
    try std.testing.expectEqual(@as(usize, txn_record_v8_size), raw.len);
    try std.testing.expectEqual(proof, (try decodeRecord(raw)).source_purpose.?);
    try std.testing.expectEqual(proof, (try completionCoordinatorRecord(raw)).source_purpose.?);
    const malformed = try a.dupe(u8, raw);
    defer a.free(malformed);
    malformed[54] = 2;
    try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(malformed));
    @memcpy(malformed, raw);
    malformed[53] = 2;
    try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(malformed));
    @memcpy(malformed, raw);
    malformed[0] = 255;
    try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(malformed));
    @memcpy(malformed, raw);
    @memset(malformed[55..87], 0);
    try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(malformed));
    mgr.deinit();
    store.close();
    store = try DocStore.open(a, path, .{});
    mgr = try TxnManager.init(a, &store);
    try mgr.requireSourcePurpose(txn_id, proof);
    const reopened_raw = try mgr.getAlloc(a, &makeRecordKey(txn_id));
    defer a.free(reopened_raw);
    try std.testing.expectEqual(proof, (try completionCoordinatorRecord(reopened_raw)).source_purpose.?);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.resolveIntents(txn_id, .committed, 11_000));
    _ = try mgr.resolveIntentsWithExtraBatch(txn_id, .committed, 11_000, .{ .source_purpose = proof });
    const stored_value = try getVisibleDocRuntime(&mgr.store, a, "a");
    defer a.free(stored_value);
    try std.testing.expectEqualStrings("source", stored_value);
    try std.testing.expectEqual(TxnStatus.committed, try mgr.getTransactionStatus(txn_id));
    const ordinary: TxnId = @splat(13);
    try mgr.initTransactionWithParticipantsCreatedAtRoleAndRetention(ordinary, 10_000, 1_000, &.{"docs:0"}, true, true);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(ordinary, 10_000, 1_000, &.{"docs:0"}, true, true, extra));
    const legacy = try mgr.getAlloc(a, &makeRecordKey(ordinary));
    defer a.free(legacy);
    try std.testing.expectEqual(@as(usize, txn_record_v6_size), legacy.len);
    try std.testing.expect((try decodeRecord(legacy)).source_purpose == null);
}

test "transaction retired source cleanup requires a permanent fence and preserves ordinary records" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-retired-source-cleanup");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var mgr = try TxnManager.init(a, &store);
    defer mgr.deinit();
    const admission = @import("source_admission.zig");
    const command: admission.Command = .{ .table_id = 7, .generation = 1, .source = "source", .epoch = "epoch", .definition_digest = @splat(1), .batch_id = "batch", .source_sequence = 1, .checkpoint = "cp", .payload_digest = @splat(2), .participant_digest = @splat(3), .accepted_at_ns = std.math.maxInt(u64) - 1 };
    const id = command.transactionId();
    try mgr.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, command.accepted_at_ns, 1_000, &.{"docs:0"}, true, true, .{ .source_purpose = command.fingerprint() });
    try std.testing.expectError(error.SourcePendingWork, mgr.cleanupRetiredSourceAttempt(command));
    const fence = blk: {
        var probe = try mgr.store.beginProbe();
        defer probe.abort();
        break :blk try admission.planAttemptRetirement(&probe, 7, command);
    };
    const fence_key = admission.fenceKey(command);
    try mgr.putValue(&fence_key, &fence.encode());
    try std.testing.expectError(error.SourcePendingWork, mgr.cleanupRetiredSourceAttempt(command));
    _ = try mgr.resolveIntentsWithExtraBatch(id, .aborted, std.math.maxInt(u64), .{ .source_purpose = command.fingerprint(), .resolved_participant = "docs:0" });
    // Ordinary retry retention cannot collect this maximum-timestamp record.
    try std.testing.expect(!try mgr.cleanupTransactionMetadataIfEligible(id, std.math.maxInt(u64), std.math.maxInt(u64)));
    try std.testing.expect(try mgr.cleanupRetiredSourceAttempt(command));
    try std.testing.expectError(error.TxnNotFound, mgr.getTransactionStatus(id));
    {
        var probe = try mgr.store.beginProbe();
        defer probe.abort();
        try std.testing.expectError(error.SourceAttemptRetired, admission.requireAttemptAllowed(&probe, command));
    }
    try mgr.initTransaction(id, 100);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.cleanupRetiredSourceAttempt(command));
    try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(id));
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

    const physical = try internal_keys.documentKeyAlloc(alloc, "existing_key");
    defer alloc.free(physical);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("value", &digest, .{});
    const revision_key = @import("document_mutation_revision.zig").keyForPhysical(physical).?;
    const revision = revision: {
        var read = try store.beginProbeTxn();
        defer read.abort();
        break :revision try @import("document_mutation_revision.zig").load(&read, revision_key);
    };
    try std.testing.expect(revision != 0);
    const exact: VersionPredicate = .{
        .key = "existing_key",
        .expected_version = 5000,
        .expected_content_digest = digest,
        .expected_mutation_revision = revision,
    };
    try mgr.checkVersionPredicates(&.{exact}, null);
    try putVisibleDoc(&store, alloc, "existing_key", "value");
    try mgr.checkVersionPredicates(&.{.{ .key = "existing_key", .expected_version = 5000, .expected_content_digest = digest }}, null);
    try std.testing.expectError(TxnError.VersionConflict, mgr.checkVersionPredicates(&.{exact}, null));

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
    const forward_prefix = try @import("db/relational/indexes/records.zig").forwardPrefix(.{ .generation = 7, .slot = 2 });
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

const DebtTestControl = struct {
    pub fn ensureActive(_: @This()) !void {}
};
fn reconcileDebtForTest(manager: *TxnManager, store: *DocStore, scope: topology_debt.Scope) !usize {
    _ = try validateDebtForTest(manager, scope);
    var pages: usize = 0;
    while (try manager.prepareTopologyDebtPage(scope, DebtTestControl{})) |page| {
        if (pages >= 2048) return error.TestUnexpectedResult;
        try std.testing.expect(page.units <= topology_debt.max_page_units);
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try page.stage(&txn);
            try txn.commit();
        }
        _ = try validateDebtForTest(manager, scope);
        pages += 1;
    }
    return pages;
}

fn validateDebtForTest(manager: *TxnManager, scope: ?topology_debt.Scope) !topology_debt.SnapshotStats {
    var read = try manager.store.beginRead();
    defer read.abort();
    return TxnManager.validateTopologyDebtSnapshot(std.testing.allocator, &read, scope, .{ .io = std.Options.debug_io, .directory = ".zig-cache", .chunk_records = 17, .chunk_bytes = 4096 });
}

test "transaction debt record audit hashes binary names in bounded windows on one immutable image" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-record-audit-names");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const name_len = 16 * 1024 + 7;
    const bytes = try alloc.alloc(u8, name_len * 3);
    defer alloc.free(bytes);
    @memset(bytes, 0xf3);
    var names: [3][]const u8 = undefined;
    for (&names, 0..) |*name, i| {
        const value = bytes[i * name_len ..][0..name_len];
        value[17] = 0;
        value[name_len - 1] = @intCast(i);
        name.* = value;
    }
    const id: TxnId = @splat(71);
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(id, 1, 1, &names, true, true);
    try manager.resolveIntents(id, .committed, 2);
    try manager.markParticipantsResolvedExtraBatch(id, &names, .{});
    var read = try store.beginReadTxn();
    defer read.abort();
    const Audit = TxnManager.TopologySnapshotAudit(@TypeOf(&read));
    const request_path = try alloc.dupe(u8, path);
    var request_path_owned = true;
    defer if (request_path_owned) alloc.free(request_path);
    var canceled = std.atomic.Value(bool).init(true);
    var audit = try Audit.init(alloc, &read, .{ .io = std.testing.io, .directory = request_path, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&canceled) }, @import("antfly_cancellation").CancellationToken.none);
    defer audit.deinit();
    @memset(request_path, 'x');
    alloc.free(request_path);
    request_path_owned = false;
    try std.testing.expectEqualStrings(path, audit.options.directory);
    try std.testing.expect(audit.options.cancellation == null);
    const member_key = makeParticipantIndexKey(participant_index_prefix, id, names[0]);
    // The retained image must not observe this later raw corruption.
    try store.put(&member_key, "wrong indexed participant");
    const Noop = @import("antfly_cancellation").CancellationToken.none;
    var paused = false;
    for (0..512) |_| {
        const complete = try audit.step(.{ .units = 1 }, Noop);
        try std.testing.expect(audit.last_units <= 1);
        if (audit.active) |*record| {
            try std.testing.expect(record.last_hashed_bytes <= 4096);
            try std.testing.expect(record.last_compared_bytes <= 4096);
            if (!paused and record.stage == .hash and record.hashed != 0 and record.hashed < record.name.len) {
                const offset = record.hashed;
                try std.testing.expectError(error.Canceled, audit.step(.{ .units = 1 }, @import("antfly_cancellation").CancellationToken.fromAtomic(&canceled)));
                try std.testing.expect(!audit.invalid);
                try std.testing.expectEqual(offset, record.hashed);
                paused = true;
            }
        }
        if (complete) break;
    } else return error.TestUnexpectedResult;
    const stats = try audit.result();
    try std.testing.expect(paused);
    try std.testing.expectEqual(@as(u64, 1), stats.records);
    try std.testing.expectEqual(@as(u64, 9 * name_len), stats.hashed_bytes);
    try std.testing.expectEqual(@as(u64, 3 * name_len), stats.compared_bytes);
    var fresh = try store.beginReadTxn();
    defer fresh.abort();
    var corrupt = try TxnManager.TopologySnapshotAudit(@TypeOf(&fresh)).init(alloc, &fresh, .{ .io = std.testing.io, .directory = path }, Noop);
    defer corrupt.deinit();
    var rejected = false;
    for (0..512) |_| {
        const complete = corrupt.step(.{ .units = 1 }, Noop) catch |err| {
            try std.testing.expectEqual(error.InvalidTxnRecord, err);
            rejected = true;
            break;
        };
        try std.testing.expect(!complete);
    }
    try std.testing.expect(rejected and corrupt.invalid);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, corrupt.result());
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, corrupt.step(.{}, Noop));
    var partial = try Audit.init(alloc, &read, .{ .io = std.testing.io, .directory = path }, Noop);
    defer partial.deinit();
    for (0..64) |_| {
        _ = try partial.step(.{ .units = 1 }, Noop);
        if (partial.active) |*record| if (record.stage == .hash and record.hashed != 0 and record.hashed < record.name.len) break;
    } else return error.TestUnexpectedResult;
    const Fail = struct {
        calls: usize = 0,
        pub fn check(self: *@This()) !void {
            self.calls += 1;
            if (self.calls >= 6) return error.Canceled;
        }
    };
    var failure: Fail = .{};
    const before = partial.stats.hashed_bytes;
    try std.testing.expectError(error.Canceled, partial.step(.{}, &failure));
    try std.testing.expectEqual(before + 4096, partial.stats.hashed_bytes);
    try std.testing.expect(partial.invalid);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, partial.result());
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, partial.step(.{}, Noop));
}

test "transaction debt record audit yields through legacy lists and large retained proofs" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-record-audit-legacy");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const names = try alloc.alloc([]u8, 600);
    defer alloc.free(names);
    var initialized: usize = 0;
    defer for (names[0..initialized]) |name| alloc.free(name);
    var total_bytes: u64 = 0;
    for (names, 0..) |*name, i| {
        name.* = try std.fmt.allocPrint(alloc, "replica-{d}", .{i});
        initialized += 1;
        total_bytes += name.len;
    }
    const id: TxnId = @splat(72);
    try manager.initTransactionWithParticipants(id, 1, names);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(&makeSidecarKey(participant_index_prefix, id));
        for (names) |name| try txn.delete(&makeParticipantIndexKey(participant_index_prefix, id, name));
        try txn.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    const Noop = @import("antfly_cancellation").CancellationToken.none;
    var audit = try TxnManager.TopologySnapshotAudit(@TypeOf(&read)).init(alloc, &read, .{ .io = std.testing.io, .directory = path, .chunk_records = 7, .fan_in = 3 }, Noop);
    defer audit.deinit();
    var spilled = false;
    var merge_passes: usize = 0;
    for (0..100_000) |_| {
        const complete = try audit.step(.{ .units = 1 }, Noop);
        try std.testing.expect(audit.last_units <= 1);
        if (audit.active) |*record| {
            try std.testing.expect(record.last_hashed_bytes <= 4096);
            if (record.proof.sort) |sort| {
                spilled = true;
                merge_passes = @max(merge_passes, sort.base.merge_passes);
                try std.testing.expect(sort.last_units <= 1);
            }
        }
        if (complete) break;
    } else return error.TestUnexpectedResult;
    const stats = try audit.result();
    try std.testing.expect(spilled and merge_passes > 1);
    try std.testing.expectEqual(@as(u64, 1), stats.records);
    try std.testing.expectEqual(total_bytes, stats.hashed_bytes);
    try std.testing.expectEqual(@as(u64, 0), stats.compared_bytes);
    _ = try TxnManager.validateTopologyDebtSnapshot(alloc, &read, null, .{ .io = std.testing.io, .directory = path });
}

test "transaction debt retained audit owns and cleans every failed scope initialization" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-record-audit-alloc");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var read = try store.beginReadTxn();
    defer read.abort();
    const Read = @TypeOf(&read);
    const Check = struct {
        fn run(a: Allocator, snapshot: Read, directory: []const u8) !void {
            const control = @import("antfly_cancellation").CancellationToken.none;
            var audit = try TxnManager.TopologySnapshotAudit(@TypeOf(snapshot)).init(a, snapshot, .{ .io = std.testing.io, .directory = directory }, control);
            defer audit.deinit();
            while (!try audit.step(.{ .units = 1 }, control)) {}
            try std.testing.expectEqual(@as(u64, 0), (try audit.result()).records);
        }
    };
    var stable = std.testing.FailingAllocator.init(alloc, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Check.run, .{ &read, path });
}

test "transaction debt snapshot audit retains one cursor across bounded turns and live writes" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-debt-snapshot-turns");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    const scope: topology_debt.Scope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .generation = @splat(4), .epoch = 1 };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var header = try topology_debt.initialize(&txn, scope);
        for (0..257) |i| {
            var id: TxnId = @splat(0);
            std.mem.writeInt(u64, id[8..16], @intCast(i), .big);
            try topology_debt.setMember(&txn, &header, id, .blocked);
        }
        try topology_debt.save(&txn, header);
        try txn.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    var points = try read.openReadScope(alloc);
    defer points.close();
    const Cursor = @typeInfo(@TypeOf(read.openCursor())).error_union.payload;
    const Counted = struct {
        read: *@TypeOf(read),
        opens: usize = 0,
        pub fn openCursor(self: *@This()) !Cursor {
            self.opens += 1;
            return self.read.openCursor();
        }
    };
    var counted: Counted = .{ .read = &read };
    const control = @import("antfly_cancellation").CancellationToken.none;
    var audit = try topology_debt.SnapshotAudit(*Counted).init(&counted, &points, scope, control);
    defer audit.deinit();
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, audit.result());
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var header = try topology_debt.require(&txn, scope);
        try topology_debt.setMember(&txn, &header, @splat(0), null);
        try topology_debt.save(&txn, header);
        try txn.commit();
    }
    var changed_read = try store.beginReadTxn();
    defer changed_read.abort();
    var changed_points = try changed_read.openReadScope(alloc);
    defer changed_points.close();
    var mixed = try topology_debt.SnapshotAudit(@TypeOf(&read)).init(&read, &points, scope, control);
    defer mixed.deinit();
    try std.testing.expectError(error.SchemaValidationJobChanged, mixed.step(&changed_points, .{}, control));
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, mixed.step(&points, .{}, control));
    var turns: usize = 0;
    while (true) {
        turns += 1;
        if (turns > 260) return error.TestUnexpectedResult;
        const complete = try audit.step(&points, .{ .records = 1 }, control);
        try std.testing.expect(audit.last_units <= 1);
        if (complete) break;
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, audit.result());
    }
    try std.testing.expectEqual(@as(usize, 259), turns);
    try std.testing.expectEqual(@as(usize, 1), counted.opens);
    const summary = try audit.result();
    try std.testing.expectEqual(@as(u64, 257), summary.members);
    try std.testing.expectEqualDeep(summary, try topology_debt.validateSnapshot(&read, &points, scope, control));
    try std.testing.expectEqual(@as(u64, 256), (try topology_debt.validateSnapshot(&changed_read, &changed_points, scope, control)).members);
}

test "transaction debt snapshot audit never qualifies rejected EOF or cancelled partial counters" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-debt-snapshot-faults");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    const scope: topology_debt.Scope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .generation = @splat(4), .epoch = 1 };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var header = try topology_debt.initialize(&txn, scope);
        header.debt = 1; // Structurally canonical, physically missing its member.
        try topology_debt.save(&txn, header);
        try txn.commit();
    }
    const Noop = @import("antfly_cancellation").CancellationToken.none;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        var points = try read.openReadScope(alloc);
        defer points.close();
        var audit = try topology_debt.SnapshotAudit(@TypeOf(&read)).init(&read, &points, scope, Noop);
        defer audit.deinit();
        try std.testing.expect(!try audit.step(&points, .{ .records = 1 }, Noop));
        try std.testing.expectError(error.InvalidTxnRecord, audit.step(&points, .{ .records = 1 }, Noop));
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, audit.step(&points, .{}, Noop));
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, audit.result());
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var header = try topology_debt.require(&txn, scope);
        header.debt = 0;
        try topology_debt.setMember(&txn, &header, @splat(1), .blocked);
        try topology_debt.setMember(&txn, &header, @splat(2), .blocked);
        try topology_debt.save(&txn, header);
        try txn.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    var points = try read.openReadScope(alloc);
    defer points.close();
    const Control = struct {
        calls: usize = 0,
        limit: usize = std.math.maxInt(usize),
        pub fn check(self: *@This()) !void {
            self.calls += 1;
            if (self.calls >= self.limit) return error.Canceled;
        }
    };
    var control: Control = .{};
    var audit = try topology_debt.SnapshotAudit(@TypeOf(&read)).init(&read, &points, scope, &control);
    defer audit.deinit();
    control.limit = control.calls + 1;
    try std.testing.expectError(error.Canceled, audit.step(&points, .{}, &control));
    try std.testing.expect(!audit.invalid and !audit.started);
    control.limit = control.calls + 4;
    try std.testing.expectError(error.Canceled, audit.step(&points, .{}, &control));
    try std.testing.expectEqual(@as(usize, 2), audit.last_units);
    control.limit = std.math.maxInt(usize);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, audit.step(&points, .{}, &control));
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, audit.result());
}

test "transaction debt atomically retains coordinator ACKs and rejects foreign generations" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-qualified-debt");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const scope: topology_debt.Scope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .generation = @splat(4), .epoch = 1 };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try TxnManager.stageTopologyDebtBegin(&txn, scope);
        try txn.commit();
    }
    const id: TxnId = @splat(7);
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(id, 10, 10, &.{ "local", "remote" }, true, true);
    try manager.resolveIntents(id, .committed, 20);
    _ = try reconcileDebtForTest(&manager, &store, scope);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(!try topology_debt.drained(&read, scope));
        try std.testing.expectEqual(@as(u64, 1), (try topology_debt.require(&read, scope)).debt);
    }
    try manager.markParticipantsResolvedExtraBatch(id, &.{"remote"}, .{});
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(!try topology_debt.drained(&read, scope));
    }
    const receipt_key = "\x00\x00__metadata__:debt_test_ack";
    try std.testing.expectError(error.InvalidParticipant, manager.resolveIntentsWithExtraBatch(id, .committed, 20, .{ .resolved_participant = "outsider" }));
    _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 20, .{
        .resolved_participant = "local",
        .completion_writes = &.{.{ .key = receipt_key, .value = "durable" }},
    });
    // Replaying local completion neither grows the ACK count nor reintroduces
    // the legacy list (which would force a cohort-sized reconciliation).
    _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 20, .{ .resolved_participant = "local" });
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try topology_debt.drained(&read, scope));
        try std.testing.expectEqualStrings("durable", try read.get(receipt_key));
        const index = try ParticipantIndex.decode(try read.get(&makeSidecarKey(participant_index_prefix, id)));
        try std.testing.expectEqual(@as(u64, 2), index.resolved);
        try std.testing.expectError(error.NotFound, read.get(&makeSidecarKey(resolved_participants_prefix, id)));
        var foreign = scope;
        foreign.epoch += 1;
        try std.testing.expectError(error.SchemaValidationJobChanged, topology_debt.drained(&read, foreign));
    }
    try std.testing.expectError(error.InvalidCommitMetadata, manager.markParticipantsResolvedExtraBatch(id, &.{"local"}, .{ .writes = &.{.{ .key = topology_debt.head_key, .value = "forged-empty" }} }));
    try std.testing.expect(!try manager.hasTopologySensitiveTransactions());
}

test "transaction debt local ACK shares commit abort and predecessor completion batches" {
    const alloc = std.testing.allocator;
    for ([_]TxnStatus{ .committed, .aborted }) |status| {
        for (0..3) |mode| {
            var backend = mem_backend.Backend.init(alloc, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(alloc, .{});
            defer runtime.deinit();
            var manager = try TxnManager.init(alloc, &runtime);
            defer manager.deinit();
            const id: TxnId = @splat(29);
            try manager.initTransactionWithParticipants(id, 10, &.{ "local", "remote", "local" });
            if (mode == 0) try manager.writeIntents(id, &.{.{ .key = "row", .value = "value" }}, &.{});
            if (mode == 2) {
                // A predecessor already decided, but has not persisted the
                // completed-intent marker. Exercise that completion branch.
                var record = try manager.loadTransactionRecord(id);
                record.status = status;
                record.intents_resolved = false;
                const bytes = try manager.encodeRecord(record);
                defer alloc.free(bytes);
                try manager.putValue(&makeRecordKey(id), bytes);
            }
            const receipt_key = "\x00\x00__metadata__:local_ack_receipt";
            _ = try manager.resolveIntentsWithExtraBatch(id, status, 20, .{
                .resolved_participant = "local",
                .completion_writes = &.{.{ .key = receipt_key, .value = "committed" }},
            });
            var read = try manager.store.beginRead();
            defer read.abort();
            const index = try ParticipantIndex.decode(try read.get(&makeSidecarKey(participant_index_prefix, id)));
            try std.testing.expectEqual(@as(u64, 2), index.enlisted);
            try std.testing.expectEqual(@as(u64, 1), index.resolved);
            try std.testing.expectEqualStrings("local", try read.get(&makeParticipantIndexKey(resolved_index_prefix, id, "local")));
            try std.testing.expectEqualStrings("committed", try read.get(receipt_key));
            try std.testing.expectError(error.NotFound, read.get(&makeSidecarKey(resolved_participants_prefix, id)));
            try std.testing.expectEqual(status, (try manager.loadTransactionRecord(id)).status);
        }
    }
}

test "transaction debt pages legacy cohorts and GCs invalidated work without hiding ACK debt" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-debt-legacy-pages");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const id: TxnId = @splat(11);
    var names: [300][16]u8 = undefined;
    var participants: [300][]const u8 = undefined;
    for (&participants, 0..) |*name, index| name.* = try std.fmt.bufPrint(&names[index], "member-{d}", .{index});
    try manager.initTransactionWithParticipants(id, 10, &participants);
    try manager.resolveIntents(id, .committed, 20);
    // A released predecessor image has the list sidecars but no indexed
    // membership/header. Do not accidentally test only new-write fast paths.
    {
        const prefix = makeSidecarKey(participant_index_prefix, id);
        const old = try backend_scan.scanPrefix(alloc, &manager.store, &prefix);
        defer backend_scan.freeResults(alloc, old);
        for (old) |entry| try store.delete(entry.key);
    }
    const scope: topology_debt.Scope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .generation = @splat(12), .epoch = 2 };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try TxnManager.stageTopologyDebtBegin(&txn, scope);
        try txn.commit();
    }
    const first = (try manager.prepareTopologyDebtPage(scope, DebtTestControl{})).?;
    try std.testing.expectEqual(@as(usize, topology_debt.max_page_units), first.units);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try first.stage(&txn);
        try txn.commit();
    }
    const stale = (try manager.prepareTopologyDebtPage(scope, DebtTestControl{})).?;
    {
        const original = try store.get(alloc, topology_debt.head_key);
        defer alloc.free(original);
        var bad = try topology_debt.Header.decode(original);
        bad.progress.enlisted += 1;
        try store.put(topology_debt.head_key, &try bad.encode());
        try std.testing.expectError(error.InvalidTxnRecord, validateDebtForTest(&manager, scope));
        try store.put(topology_debt.head_key, original);
        bad = try topology_debt.Header.decode(original);
        bad.progress.offset += 1;
        try store.put(topology_debt.head_key, &try bad.encode());
        try std.testing.expectError(error.InvalidTxnRecord, validateDebtForTest(&manager, scope));
        try store.put(topology_debt.head_key, original);
    }
    try manager.markParticipantResolved(id, participants[0]);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.SchemaValidationPreparationChanged, stale.stage(&txn));
    }
    const pages = try reconcileDebtForTest(&manager, &store, scope);
    try std.testing.expect(pages > 8);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(!try topology_debt.drained(&read, scope));
        try std.testing.expectEqual(@as(u64, 1), (try topology_debt.require(&read, scope)).debt);
        try std.testing.expect(!try topology_debt.hasPrefix(&read, &topology_debt.orphanPrefix(scope.generation)));
        try std.testing.expect(!try topology_debt.hasPrefix(&read, &topology_debt.seenPrefix(scope.generation)));
    }
    var offset: usize = 0;
    while (offset < participants.len) {
        const end = @min(participants.len, offset + 64);
        try manager.markParticipantsResolvedExtraBatch(id, participants[offset..end], .{});
        offset = end;
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try topology_debt.drained(&txn, scope));
        try topology_debt.retire(&txn, scope);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.SchemaValidationJobChanged, TxnManager.stageTopologyDebtBegin(&txn, scope));
    }
}

test "transaction debt snapshot rejects canonical empty claims and orphan members" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();
    var manager = try TxnManager.init(alloc, &runtime);
    defer manager.deinit();
    const id: TxnId = @splat(31);
    try manager.initTransactionWithParticipants(id, 10, &.{"local"});
    {
        _ = try validateDebtForTest(&manager, null);
        const key = makeSidecarKey(participant_index_prefix, id);
        const original = try manager.getAlloc(alloc, &key);
        defer alloc.free(original);
        var forged_index: [17]u8 = undefined;
        (ParticipantIndex{ .enlisted = 1, .resolved = 1 }).encode(&forged_index);
        try manager.putValue(&key, &forged_index);
        try std.testing.expectError(error.InvalidTxnRecord, validateDebtForTest(&manager, null));
        try manager.putValue(&key, original);
    }
    const scope: topology_debt.Scope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .generation = @splat(32), .epoch = 3 };
    {
        var txn = try manager.store.beginBatch();
        errdefer txn.abort();
        try TxnManager.stageTopologyDebtBegin(&txn, scope);
        try txn.commit();
    }
    _ = try validateDebtForTest(&manager, scope);
    const head_bytes = try manager.getAlloc(alloc, topology_debt.head_key);
    defer alloc.free(head_bytes);
    var forged = try topology_debt.Header.decode(head_bytes);
    forged.phase = .ready;
    forged.after = null;
    forged.debt = 0;
    {
        var txn = try manager.store.beginBatch();
        errdefer txn.abort();
        try txn.delete(&topology_debt.memberKey(scope.generation, id));
        try topology_debt.save(&txn, forged);
        try txn.commit();
    }
    // Header/count/directory are internally canonical, but the retained
    // pending coordinator disproves the apparent empty/ready claim.
    try std.testing.expectError(error.InvalidTxnRecord, validateDebtForTest(&manager, scope));
    {
        var txn = try manager.store.beginBatch();
        errdefer txn.abort();
        try txn.put(topology_debt.head_key, head_bytes);
        try txn.put(&topology_debt.memberKey(scope.generation, id), &.{1});
        try txn.commit();
    }
    const index_key = makeSidecarKey(participant_index_prefix, id);
    var invalid_index: [17]u8 = undefined;
    (ParticipantIndex{ .enlisted = 1, .resolved = 1 }).encode(&invalid_index);
    const old_index = try manager.getAlloc(alloc, &index_key);
    defer alloc.free(old_index);
    try manager.putValue(&index_key, &invalid_index);
    try std.testing.expectError(error.InvalidTxnRecord, validateDebtForTest(&manager, scope));
    try manager.putValue(&index_key, old_index);
    {
        var txn = try manager.store.beginBatch();
        errdefer txn.abort();
        var h = try topology_debt.Header.decode(head_bytes);
        h.debt += 1;
        try topology_debt.save(&txn, h);
        try txn.put(&topology_debt.memberKey(scope.generation, @splat(33)), &.{1});
        try txn.commit();
    }
    try std.testing.expectError(error.InvalidTxnRecord, validateDebtForTest(&manager, scope));
}

test "transaction debt dirty predecessors invalidate later partial work and reopen GC" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "txn-earlier-dirty");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var manager = try TxnManager.init(alloc, &store);
    defer manager.deinit();
    const first: TxnId = @splat(1);
    const later: TxnId = @splat(2);
    const names = [_][]const u8{ "a", "b", "c", "d" };
    try manager.initTransactionWithParticipants(first, 10, &names);
    try manager.resolveIntents(first, .committed, 20);
    try manager.markParticipantsResolvedExtraBatch(first, &names, .{});
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const participants = try arena.allocator().alloc([]const u8, 300);
    for (participants, 0..) |*name, i| name.* = try std.fmt.allocPrint(arena.allocator(), "p-{d}", .{i});
    try manager.initTransactionWithParticipants(later, 10, participants);
    try manager.resolveIntents(later, .committed, 20);
    try removeParticipantIndexForTest(&manager, later);
    const scope: topology_debt.Scope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .generation = @splat(34), .epoch = 4 };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try TxnManager.stageTopologyDebtBegin(&txn, scope);
        try txn.commit();
    }
    {
        const page = (try manager.prepareTopologyDebtPage(scope, DebtTestControl{})).?;
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try page.stage(&txn);
        try txn.commit();
    }
    const legacy_key = makeSidecarKey(resolved_participants_prefix, first);
    const legacy = try encodeParticipantList(alloc, &.{"a"});
    defer alloc.free(legacy);
    try manager.putValue(&legacy_key, legacy);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const h = try topology_debt.require(&read, scope);
        try std.testing.expect(h.pending == null);
        try std.testing.expect(try topology_debt.hasPrefix(&read, &topology_debt.orphanPrefix(scope.generation)));
    }
    _ = try reconcileDebtForTest(&manager, &store, scope);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var h = try topology_debt.require(&txn, scope);
        try std.testing.expectEqual(@as(u64, 1), h.debt);
        h.phase = .gc; // A valid GC turn after its last orphan was reclaimed.
        try topology_debt.save(&txn, h);
        try txn.commit();
    }
    try manager.putValue(&legacy_key, legacy);
    _ = try reconcileDebtForTest(&manager, &store, scope);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const h = try topology_debt.require(&read, scope);
        try std.testing.expectEqual(topology_debt.Phase.ready, h.phase);
        try std.testing.expectEqual(@as(u64, 1), h.debt);
        try std.testing.expect(try topology_debt.member(&read, scope.generation, first) == null);
    }
}

test "transaction debt malformed released status tags remain typed corruption" {
    for ([_]usize{ txn_record_v0_size, txn_record_v1_size, txn_record_v2_size, txn_record_v3_size, txn_record_v4_size, txn_record_v5_size, txn_record_v6_size }) |size| {
        var bytes: [txn_record_v6_size]u8 = @splat(0);
        for ([_]u8{ 3, 127, 255 }) |status| {
            bytes[0] = status;
            try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(bytes[0..size]));
        }
    }
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

fn removeParticipantIndexForTest(manager: *TxnManager, id: TxnId) !void {
    const entries = try manager.scanPrefix(manager.alloc, &makeSidecarKey(participant_index_prefix, id));
    defer backend_scan.freeResults(manager.alloc, entries);
    const deletes = try manager.alloc.alloc([]const u8, entries.len);
    defer manager.alloc.free(deletes);
    for (entries, 0..) |entry, i| deletes[i] = entry.key;
    try manager.applyBatch(&.{}, deletes, null);
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
    // Model a released predecessor, not a newly indexed BEGIN.
    try removeParticipantIndexForTest(&mgr, txn);
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
            try removeParticipantIndexForTest(&mgr, txn);
            try mgr.markParticipantResolved(txn, "a");
            mgr.alloc = failing;
            defer mgr.alloc = alloc;
            try mgr.markParticipantsResolvedExtraBatch(txn, &.{ "b", "c" }, .{});
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "external prepared coordinator retains LSM vote across cold timeout and requires exact resolution authority" {
    const alloc = std.testing.allocator;
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    for ([_]TxnStatus{ .committed, .aborted }) |decision| {
        const path = try tempTestPath(alloc, if (decision == .committed) "external-prepared-commit" else "external-prepared-abort");
        defer alloc.free(path);
        cleanupTestDir(path);
        defer cleanupTestDir(path);
        const id: TxnId = @splat(7);
        {
            var store = try DocStore.open(alloc, path, .{});
            defer store.close();
            var manager = try TxnManager.init(alloc, &store);
            defer manager.deinit();
            try putVisibleDoc(&store, alloc, "held", "before");
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{ "coordinator", "participant" }, true, true, .{ .external_decision_owner = owner });
            try manager.writeIntentsExtraBatch(id, &.{.{ .key = "held", .value = "committed-value" }}, &.{}, .{ .external_decision_owner = owner });
            try std.testing.expect(!try manager.hasStatementReadDebt());
            try std.testing.expectError(error.ExternalTransactionDecisionRequired, manager.writeIntents(id, &.{}, &.{}));
            var other = owner;
            other.operation_id[0] ^= 1;
            try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.writeIntentsExtraBatch(id, &.{}, &.{}, .{ .external_decision_owner = other }));
            const record = try manager.loadTransactionRecord(id);
            try std.testing.expect(record.external_decision_owner.?.eql(owner));
            const bytes = try manager.encodeRecord(record);
            defer alloc.free(bytes);
            try std.testing.expectEqual(@as(usize, txn_record_external_size), bytes.len);
            try std.testing.expect((try decodeRecord(bytes)).external_decision_owner.?.eql(owner));
            bytes[49] = 2;
            try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(bytes));
        }
        {
            var store = try DocStore.open(alloc, path, .{});
            defer store.close();
            var manager = try TxnManager.init(alloc, &store);
            defer manager.deinit();
            const stats = try manager.recoverTransactions(1000, 2000);
            try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
            try std.testing.expectEqual(@as(u64, 1), stats.kept_recent_pending);
            try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(id));
            try std.testing.expect(!try manager.hasStatementReadDebt());
            var guard = IntentReadGuard.init(alloc, null);
            defer guard.deinit();
            var old_read = try store.beginReadTxn();
            defer old_read.abort();
            try guard.check(&old_read, "held");
            const before = try getVisibleDoc(&store, alloc, "held");
            defer alloc.free(before);
            try std.testing.expectEqualStrings("before", before);
            if (decision == .committed) {
                try std.testing.expectError(error.ExternalTransactionCommitNotActivated, manager.resolveIntentsWithExtraBatch(id, decision, 2001, .{ .external_decision_owner = owner }));
                try manager.activateExternalDecision(id, owner, .{});
                try manager.activateExternalDecision(id, owner, .{});
                try std.testing.expect(try manager.hasStatementReadDebt());
                try guard.check(&old_read, "held");
                var current_read = try store.beginReadTxn();
                defer current_read.abort();
                try std.testing.expectError(error.IntentConflict, guard.check(&current_read, "held"));
                try std.testing.expectError(error.DecisionConflict, manager.resolveIntentsWithExtraBatch(id, .aborted, 2001, .{ .external_decision_owner = owner }));
            }
            try std.testing.expectError(error.ExternalTransactionDecisionRequired, manager.resolveIntents(id, decision, 2001));
            var other = owner;
            other.admission_digest[0] ^= 1;
            try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.resolveIntentsWithExtraBatch(id, decision, 2001, .{ .external_decision_owner = other }));
            _ = try manager.resolveIntentsWithExtraBatch(id, decision, 2001, .{ .external_decision_owner = owner });
            try std.testing.expectEqual(decision, try manager.getTransactionStatus(id));
            try std.testing.expect(!try manager.hasStatementReadDebt());
            // Terminal propagation/recovery is the ordinary path. It may not
            // need the private admission again and never replays row effects.
            try manager.resolveIntents(id, decision, 2001);
            if (decision == .committed) {
                const value = try getVisibleDoc(&store, alloc, "held");
                defer alloc.free(value);
                try std.testing.expectEqualStrings("committed-value", value);
            } else {
                const value = try getVisibleDoc(&store, alloc, "held");
                defer alloc.free(value);
                try std.testing.expectEqualStrings("before", value);
            }
        }
    }
}

test "external prepared statement admission skips large intent cohorts and retains ordinary barriers" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "external-statement-admission");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const external: TxnId = @splat(7);
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(external, 10, 1, &.{"owner"}, true, true, .{ .external_decision_owner = owner });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const intents = try arena.allocator().alloc(WriteIntent, 1024);
    for (intents, 0..) |*intent, index| intent.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "row-{d:0>5}", .{index}), .value = "next" };
    try manager.writeIntentsExtraBatch(external, intents, &.{}, .{ .external_decision_owner = owner });
    try std.testing.expect(!try manager.hasStatementReadDebt());
    // Sort after the external cohort: seeking past its rows must still find
    // ordinary uncertainty. The separate cold-owner campaign checks reopen.
    const ordinary: TxnId = @splat(8);
    try manager.initTransactionWithParticipants(ordinary, 10, &.{"ordinary"});
    try manager.writeIntents(ordinary, &.{.{ .key = "ordinary", .value = "next" }}, &.{});
    try std.testing.expect(try manager.hasStatementReadDebt());
    try manager.resolveIntents(ordinary, .aborted, 20);
    try std.testing.expect(!try manager.hasStatementReadDebt());
    var old = try store.beginReadTxn();
    defer old.abort();
    try manager.activateExternalDecision(external, owner, .{});
    try std.testing.expect(try manager.hasStatementReadDebt());
    try std.testing.expect(!try statementReadHasDebt(&old));
    _ = try manager.resolveIntentsWithExtraBatch(external, .committed, 20, .{ .external_decision_owner = owner });
    try std.testing.expect(!try manager.hasStatementReadDebt());
}

test "external prepared concurrent activation barriers survive restart and retire exactly once" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "external-read-barriers");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const ids = [_]TxnId{ @splat(71), @splat(72) };
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        for (ids) |id| {
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{"owner"}, true, true, .{ .external_decision_owner = owner });
            try manager.writeIntentsExtraBatch(id, &.{}, &.{}, .{ .external_decision_owner = owner });
        }
        var before = try store.beginReadTxn();
        defer before.abort();
        for (ids) |id| {
            try manager.activateExternalDecision(id, owner, .{});
            try manager.activateExternalDecision(id, owner, .{});
        }
        try checkExternalReadAdmission(&before);
        var current = try store.beginReadTxn();
        defer current.abort();
        try std.testing.expectEqual(@as(u64, 2), try externalReadBarrierCount(&current));
        try std.testing.expect(try manager.hasStatementReadDebt());
        try std.testing.expectError(error.IntentConflict, checkExternalReadAdmission(&current));
    }
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        for (ids, 0..) |id, i| {
            _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 20, .{ .external_decision_owner = owner });
            try manager.resolveIntents(id, .committed, 20);
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u64, 1 - i), try externalReadBarrierCount(&read));
            try std.testing.expectEqual(i == 0, try manager.hasStatementReadDebt());
            if (i == 0) try std.testing.expectError(error.IntentConflict, checkExternalReadAdmission(&read)) else try checkExternalReadAdmission(&read);
            var absent: ?[]const u8 = null;
            if (i == 0) {
                try std.testing.expectError(error.IntentConflict, captureServingValue(&store, "absent", &absent));
            } else {
                var lease = try captureServingValue(&store, "absent", &absent);
                defer lease.abort();
                try std.testing.expect(absent == null);
            }
        }
    }
}

test "external prepared abort retains BEGIN authority across timeout recovery and restart" {
    const a = std.testing.allocator;
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const id: TxnId = @splat(82);
    for ([_]bool{ false, true }) |timed_out| {
        const path = try tempTestPath(a, if (timed_out) "external-abort-timeout" else "external-abort-begin");
        defer a.free(path);
        cleanupTestDir(path);
        defer cleanupTestDir(path);
        {
            var store = try DocStore.open(a, path, .{});
            defer store.close();
            var manager = try TxnManager.init(a, &store);
            defer manager.deinit();
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{ "owner", "follower" }, true, true, .{ .external_decision_owner = owner });
            if (timed_out) {
                const stats = try manager.recoverTransactions(1000, 2000);
                try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
                try std.testing.expectError(error.ExternalTransactionDecisionRequired, manager.resolveIntents(id, .aborted, 20));
            }
            _ = try manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = owner });
            _ = try manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = owner });
            var other = owner;
            other.operation_id[0] ^= 1;
            try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = other }));
        }
        {
            var store = try DocStore.open(a, path, .{});
            defer store.close();
            var manager = try TxnManager.init(a, &store);
            defer manager.deinit();
            const record = try manager.loadTransactionRecord(id);
            try std.testing.expectEqual(TxnStatus.aborted, record.status);
            try std.testing.expect(!record.prepared);
            try std.testing.expect(record.external_decision_owner.?.eql(owner));
            const unresolved = try manager.getUnresolvedParticipants(a, id);
            defer freeParticipantList(a, unresolved);
            try std.testing.expectEqual(@as(usize, 2), unresolved.len);
            var read = try store.beginReadTxn();
            defer read.abort();
            try checkExternalReadAdmission(&read);
        }
    }
}

test "external prepared missing abort fences late enlistment and reclaims to a durable expiry floor" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "external-fence-expiry");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(83);
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 };
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        _ = try manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = owner, .resolved_participant = "self", .completion_writes = &.{.{ .key = "abort-marker", .value = "first" }} });
        try std.testing.expectError(error.ExternalTransactionDecisionRequired, manager.initTransactionWithParticipantsCreatedAtAndRole(id, 10, 1, &.{"self"}, false));
        try std.testing.expect(!(try manager.cleanupTransactionMetadataIfEligible(id, 99, 0)));
        // An advanced ordinary retention clock cannot retire a live admission.
        try std.testing.expect(!(try manager.cleanupTransactionMetadataIfEligible(id, 99, 1000)));
        try std.testing.expect(try manager.cleanupTransactionMetadataIfEligible(id, 101, 0));
        try std.testing.expectEqual(@as(u64, 100), try manager.externalExpiryFloor());
    }
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try std.testing.expectEqual(@as(u64, 100), try manager.externalExpiryFloor());
        // Even BEGIN is fenced after reclamation, independent of host clocks.
        try std.testing.expectError(error.ExternalTransactionAdmissionExpired, manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{"self"}, false, true, .{ .external_decision_owner = owner }));
        try std.testing.expectError(error.TxnNotFound, manager.loadTransactionRecord(id));
        // An ordinary transaction cannot be adopted by any external decision.
        try manager.initTransactionWithParticipantsCreatedAtAndRole(id, 10, 1, &.{"self"}, false);
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.writeIntentsExtraBatch(id, &.{.{ .key = "late", .value = "{}" }}, &.{}, .{ .external_decision_owner = owner }));
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = owner }));
        try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(id));
        try std.testing.expect(!(try manager.hasIntents(id)));
        const missing: TxnId = @splat(84);
        _ = try manager.resolveIntentsWithExtraBatch(missing, .aborted, 20, .{ .external_decision_owner = owner, .completion_writes = &.{.{ .key = "abort-marker", .value = "replay" }} });
        try std.testing.expectError(error.TxnNotFound, manager.loadTransactionRecord(missing));
        const marker = try manager.getAlloc(a, "abort-marker");
        defer a.free(marker);
        try std.testing.expectEqualStrings("replay", marker);
        // New admissions above the floor remain available.
        var fresh = owner;
        fresh.operation_id[0] ^= 1;
        fresh.expires_at_unix_ns = 200;
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(missing, 30, 30, &.{"self"}, false, true, .{ .external_decision_owner = fresh });
        try manager.writeIntentsExtraBatch(missing, &.{.{ .key = "fresh", .value = "{}" }}, &.{}, .{ .external_decision_owner = fresh });
        try std.testing.expect(try manager.hasIntents(missing));
    }
}

test "external prepared enlistment survives cold recovery before the first vote" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "external-enlistment");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(86);
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 };
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{"self"}, false, true, .{ .external_decision_owner = owner });
    }
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        const record = try manager.loadTransactionRecord(id);
        try std.testing.expect(!record.prepared);
        try std.testing.expect(record.external_decision_owner.?.eql(owner));
        const stats = try manager.recoverTransactions(1000, 2000);
        try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
        try std.testing.expectEqual(TxnStatus.pending, try manager.getTransactionStatus(id));
        try std.testing.expectError(error.ExternalTransactionDecisionRequired, manager.initTransactionWithParticipantsCreatedAtAndRole(id, 10, 1, &.{"self"}, false));
        var wrong = owner;
        wrong.operation_id[0] ^= 1;
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{"self"}, false, true, .{ .external_decision_owner = wrong }));
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{"self"}, false, true, .{ .external_decision_owner = owner });
        try manager.writeIntentsExtraBatch(id, &.{.{ .key = "row", .value = "held" }}, &.{}, .{ .external_decision_owner = owner });
        _ = try manager.resolveIntentsWithExtraBatch(id, .aborted, 2000, .{ .external_decision_owner = owner });
        try std.testing.expect(!(try manager.hasIntents(id)));
    }
}

test "external decision ACK validates exact terminal owner before changing retention" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "external-owner-ack");
    cleanupTestDir(path);
    defer a.free(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(87);
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 };
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{"self"}, true, true, .{ .external_decision_owner = owner });
    try std.testing.expectError(error.ExternalTransactionDecisionRequired, manager.markParticipantsResolvedExtraBatch(id, &.{"self"}, .{ .external_decision_owner = owner }));
    _ = try manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = owner });
    var wrong = owner;
    wrong.operation_id[0] ^= 1;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.markParticipantsResolvedExtraBatch(id, &.{"self"}, .{ .external_decision_owner = wrong }));
    const pending = try manager.getResolvedParticipants(a, id);
    defer freeParticipantList(a, pending);
    try std.testing.expectEqual(@as(usize, 0), pending.len);
    try manager.markParticipantsResolvedExtraBatch(id, &.{"self"}, .{ .external_decision_owner = owner });
    const resolved = try manager.getResolvedParticipants(a, id);
    defer freeParticipantList(a, resolved);
    try std.testing.expectEqual(@as(usize, 1), resolved.len);
}

comptime {
    if (@import("builtin").is_test) _ = @import("pessimistic_lock/native_test.zig");
}

test "SQL relation lifetime native participant records pin actual images across every decision mutation" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(a, &runtime);
    defer store.close();
    const pin: NativePhysicalPin = .{ .physical_root_incarnation = std.math.maxInt(u128) - 3, .table_id = 7, .shard_id = 172, .range_id = 272, .data_generation = 9 };
    store.native_relation_pin = pin;
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    try std.testing.expect(manager.native_relation_pin.?.eql(pin));
    const id: TxnId = @splat(11);
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 };
    const extra: MutationExtraBatch = .{ .external_decision_owner = owner, .native_pin = pin };
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 1, &.{"self"}, true, true, extra);
    {
        const raw = try manager.getAlloc(a, &makeRecordKey(id));
        defer a.free(raw);
        try std.testing.expectEqual(@as(usize, txn_record_external_pinned_size), raw.len);
        const record = try decodeRecord(raw);
        try std.testing.expect(record.native_pin.?.eql(pin));
        try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(raw[0 .. raw.len - 1]));
    }
    try manager.writeIntentsExtraBatch(id, &.{.{ .key = "row", .value = "held" }}, &.{}, extra);
    var wrong = pin;
    wrong.data_generation += 1;
    const wrong_extra: MutationExtraBatch = .{ .external_decision_owner = owner, .native_pin = wrong };
    {
        var pinned = try store.beginReadTxn();
        defer pinned.abort();
        try TxnManager.requireCleanNativeRecoveryInView(a, &pinned, pin);
        try std.testing.expectError(error.SqlRelationRecoveryIncomplete, TxnManager.requireCleanNativeRecoveryInView(a, &pinned, wrong));
    }
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.writeIntentsExtraBatch(id, &.{}, &.{}, wrong_extra));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.activateExternalDecision(id, owner, wrong_extra));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = owner, .native_pin = wrong }));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.markParticipantsResolvedExtraBatch(id, &.{"self"}, wrong_extra));
    // A copied record under a successor empty generation cannot be adopted by
    // presenting its still-valid decision owner or omitting request pin facts.
    manager.native_relation_pin = wrong;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.getTransactionStatus(id));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.writeIntentsExtraBatch(id, &.{}, &.{}, .{ .external_decision_owner = owner }));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.activateExternalDecision(id, owner, .{}));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{ .external_decision_owner = owner }));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.markParticipantsResolvedExtraBatch(id, &.{"self"}, .{ .external_decision_owner = owner }));
    manager.native_relation_pin = pin;
    try manager.activateExternalDecision(id, owner, extra);
    _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 20, .{ .external_decision_owner = owner, .native_pin = pin });
    try manager.markParticipantsResolvedExtraBatch(id, &.{"self"}, extra);
    var read = try store.beginReadTxn();
    defer read.abort();
    const participant = (try externalParticipant(&read, id)).?;
    try std.testing.expect(participant.native_pin.?.eql(pin));
    try std.testing.expectEqual(TxnStatus.committed, participant.status);
    try std.testing.expect(participant.owner.?.eql(owner));
}

test "prepared graph intent envelope preserves ordinary bytes and binds attachment to the same immutable row revision" {
    const a = std.testing.allocator;
    const plain: WriteIntent = .{ .key = "a", .value = "{\"n\":1}" };
    const original = try IntentValue.encode(a, plain);
    defer a.free(original);
    try std.testing.expectEqualSlices(u8, "\x00{\"n\":1}", original);
    var graph = plain;
    graph.prepared_graph_effects = "immutable-native-graph-effects";
    const encoded = try IntentValue.encode(a, graph);
    defer a.free(encoded);
    const decoded = try IntentValue.decode(encoded);
    try std.testing.expectEqualStrings(plain.value.?, decoded.value.?);
    try std.testing.expectEqualStrings(graph.prepared_graph_effects.?, decoded.prepared_graph_effects.?);
    try std.testing.expectEqual(try intentAdmissionBytes(plain) + graph.prepared_graph_effects.?.len, try intentAdmissionBytes(graph));
    var deleted = graph;
    deleted.value = null;
    try std.testing.expectError(error.InvalidPreparedGraphEffects, IntentValue.encode(a, deleted));
    try std.testing.expectError(error.InvalidTxnRecord, IntentValue.decode(encoded[0..5]));
    // An attacker-controlled recursive wrapper must never consume unbounded
    // decoder stack or produce nested attachments with ambiguous ownership.
    const nested = try a.alloc(u8, 6 + encoded.len);
    defer a.free(nested);
    nested[0] = 4;
    std.mem.writeInt(u32, nested[1..5], 1, .little);
    nested[5] = 'x';
    @memcpy(nested[6..], encoded);
    try std.testing.expectError(error.InvalidTxnRecord, IntentValue.decode(nested));
}

test "SQL relation lifetime native missing BEGIN retention snapshots preserve exact singleton and full coordinator rosters across cold ACK" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "native-retention-roster");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const pin: NativePhysicalPin = .{ .physical_root_incarnation = 17, .table_id = 7, .shard_id = 172, .range_id = 272, .data_generation = 1 };
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const id: TxnId = @splat(4);
    for (0..2) |pass| {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        store.native_relation_pin = pin;
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        if (pass == 0) _ = try manager.resolveIntentsWithExtraBatch(id, .aborted, 10, .{ .external_decision_owner = owner, .native_pin = pin, .resolved_participant = "local" });
        var retained = try manager.externalRetentionSnapshot(a, id, owner, pin);
        defer retained.deinit(a);
        try std.testing.expect(retained.status == .aborted and !retained.coordinator and !retained.prepared and retained.intents_resolved);
        try std.testing.expectEqual(@as(usize, 1), retained.participants.len);
        try std.testing.expectEqualStrings("local", retained.participants[0]);
        var changed = owner;
        changed.operation_id = @splat(9);
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.externalRetentionSnapshot(a, id, changed, pin));
        var wrong = pin;
        wrong.data_generation += 1;
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.externalRetentionSnapshot(a, id, owner, wrong));
        // A normal global ACK remains rejected atomically by this singleton;
        // the authenticated upper layer must select its actual local roster.
        try std.testing.expectError(error.InvalidParticipant, manager.markParticipantsResolvedExtraBatch(id, &.{ "local", "foreign" }, .{ .external_decision_owner = owner, .native_pin = pin }));
        const unresolved = try manager.getUnresolvedParticipants(a, id);
        defer freeParticipantList(a, unresolved);
        try std.testing.expectEqual(@as(usize, 1), unresolved.len);
        if (pass == 1) {
            try manager.markParticipantsResolvedExtraBatch(id, &.{"local"}, .{ .external_decision_owner = owner, .native_pin = pin });
            try manager.markParticipantsResolvedExtraBatch(id, &.{"local"}, .{ .external_decision_owner = owner, .native_pin = pin });
            const resolved = try manager.getResolvedParticipants(a, id);
            defer freeParticipantList(a, resolved);
            try std.testing.expectEqual(@as(usize, 1), resolved.len);
        }
        const coordinator: TxnId = @splat(5);
        if (pass == 0) {
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(coordinator, 20, 19, &.{ "local", "remote" }, true, true, .{ .external_decision_owner = owner, .native_pin = pin });
            _ = try manager.resolveIntentsWithExtraBatch(coordinator, .aborted, 30, .{ .external_decision_owner = owner, .native_pin = pin, .resolved_participant = "local" });
        }
        var global = try manager.externalRetentionSnapshot(a, coordinator, owner, pin);
        defer global.deinit(a);
        try std.testing.expect(global.coordinator and global.status == .aborted);
        try std.testing.expectEqual(@as(usize, 2), global.participants.len);
        if (pass == 1) try manager.markParticipantsResolvedExtraBatch(coordinator, &.{ "local", "remote" }, .{ .external_decision_owner = owner, .native_pin = pin });
    }
}

test "SQL relation lifetime native completed ordinary close history" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "native-ordinary-close-history");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const pin: NativePhysicalPin = .{ .physical_root_incarnation = 17, .table_id = 7, .shard_id = 172, .range_id = 272, .data_generation = 2 };
    const id: TxnId = @splat(91);
    for (0..2) |pass| {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        store.native_relation_pin = pin;
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        if (pass == 0) {
            try manager.initTransactionWithParticipantsCreatedAtAndRole(id, 10, 1, &.{ "self", "peer" }, true);
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectError(error.SqlRelationRecoveryIncomplete, TxnManager.requireCleanNativeRecoveryInView(a, &read, pin));
            }
            try manager.writeIntents(id, &.{.{ .key = "validation-control", .value = "{}" }}, &.{});
            try manager.resolveIntents(id, .committed, 20);
            try manager.markParticipantResolved(id, "self");
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectError(error.SqlRelationRecoveryIncomplete, TxnManager.requireCleanNativeRecoveryInView(a, &read, pin));
            }
            try manager.markParticipantResolved(id, "peer");
        }
        const record = try manager.loadTransactionRecord(id);
        try std.testing.expect(record.status == .committed and record.intents_resolved_known and record.intents_resolved);
        try std.testing.expect(record.external_decision_owner == null and record.native_pin == null);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try TxnManager.requireCleanNativeRecoveryInView(a, &read, pin);
        }
        // A completed journal never licenses leftover effects, including the
        // schema-lease family not covered by the generic lifetime-debt probe.
        inline for (.{ intents_prefix, intent_locks_prefix, read_guards_prefix, read_members_prefix, schema_leases_prefix }) |prefix| {
            const effect = if (comptime std.mem.eql(u8, prefix, intents_prefix)) prefix ++ @as([16]u8, @splat(91)) ++ ":row" else if (comptime std.mem.eql(u8, prefix, read_guards_prefix)) prefix ++ @as([4]u8, @splat(0)) ++ @as([16]u8, @splat(91)) else prefix ++ @as([16]u8, @splat(91));
            try store.put(effect, if (comptime std.mem.eql(u8, prefix, intent_locks_prefix)) &id else "leftover");
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectError(error.InvalidTxnRecord, TxnManager.requireCleanNativeRecoveryInView(a, &read, pin));
            }
            try store.delete(effect);
        }
        const orphan = makeSidecarKey(schema_leases_prefix, @splat(92));
        try store.put(&orphan, "orphan");
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.SqlRelationRecoveryIncomplete, TxnManager.requireCleanNativeRecoveryInView(a, &read, pin));
        }
        try store.delete(&orphan);
        // Existing external admission custody remains root-bound even once
        // terminal and locally resolved; ordinary-history rules cannot adopt it.
        const external: TxnId = @splat(93);
        const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 };
        if (pass == 0) {
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(external, 30, 1, &.{"self"}, true, true, .{ .external_decision_owner = owner, .native_pin = pin });
            _ = try manager.resolveIntentsWithExtraBatch(external, .aborted, 40, .{ .external_decision_owner = owner, .native_pin = pin });
        }
        var wrong = pin;
        wrong.data_generation += 1;
        var read = try store.beginReadTxn();
        defer read.abort();
        try TxnManager.requireCleanNativeRecoveryInView(a, &read, pin);
        try std.testing.expectError(error.SqlRelationRecoveryIncomplete, TxnManager.requireCleanNativeRecoveryInView(a, &read, wrong));
        try std.testing.expect(try TxnManager.hasRelationLifetimeDebtInView(a, &read));
    }
}

test "time series prepared revision scheme survives reopening and generic recovery" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-revision-recovery");
    defer a.free(path);
    defer cleanupTestDir(path);
    for ([_]bool{ false, true }) |modern| {
        cleanupTestDir(path);
        const id: TxnId = @splat(7);
        {
            var store = try DocStore.open(a, path, .{});
            defer store.close();
            var manager = try TxnManager.init(a, &store);
            defer manager.deinit();
            try manager.initTransaction(id, 1);
            try manager.writeIntentsExtraBatch(id, &.{.{ .key = "row", .value = "{}" }}, &.{}, .{ .document_revisions = modern });
            try manager.writeIntentsExtraBatch(id, &.{.{ .key = "row", .value = "{\"corrected\":true}" }}, &.{}, .{ .document_revisions = !modern });
            const raw = try store.get(a, &makeRecordKey(id));
            defer a.free(raw);
            try std.testing.expectEqual(@as(usize, if (modern) txn_record_v7_size else txn_record_v6_size), raw.len);
            try std.testing.expectEqual(modern, (try decodeRecord(raw)).document_revisions);
        }
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.resolveIntents(id, .committed, 0);
        const key = try internal_keys.ttlKeyAlloc(a, "row");
        defer a.free(key);
        const raw = try store.get(a, key);
        defer a.free(raw);
        try std.testing.expectEqual(@as(u64, if (modern) 1 else 0), ttl.writeVersion(raw).?);
        try std.testing.expectEqual(@as(usize, if (modern) 24 else 8), raw.len);
        try std.testing.expectEqual(@as(?i128, 0), ttl.sourceTimestamp(raw));
    }
}

test "time series source language reserves signed source slots before voting" {
    const intent: WriteIntent = .{ .key = "row", .value = "{}" };
    const legacy = try retainedIntentBytes(std.testing.allocator, intent);
    try std.testing.expectEqual(legacy, try retainedIntentBytesForLanguage(std.testing.allocator, intent, false));
    try std.testing.expectEqual(legacy + 16, try retainedIntentBytesForLanguage(std.testing.allocator, intent, true));
    const deleted: WriteIntent = .{ .key = "row", .value = null };
    try std.testing.expectEqual(try retainedIntentBytes(std.testing.allocator, deleted), try retainedIntentBytesForLanguage(std.testing.allocator, deleted, true));
}

test "time series record codec keeps main revision bytes and SQL owner native pin independent" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{});
    defer runtime.deinit();
    var store = try DocStore.openRuntime(a, &runtime);
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 };
    const pin: NativePhysicalPin = .{ .physical_root_incarnation = 19, .table_id = 7, .shard_id = 8, .range_id = 9, .data_generation = 1 };
    for ([_]bool{ false, true }) |modern| for (0..3) |kind| {
        const record: TxnRecord = .{
            .status = .pending,
            .begin_timestamp = 1,
            .commit_version = 0,
            .created_at = 2,
            .finalized_at = 0,
            .document_revisions = modern,
            .external_decision_owner = if (kind > 0) owner else null,
            .native_pin = if (kind == 2) pin else null,
        };
        const encoded = try manager.encodeRecord(record);
        defer a.free(encoded);
        try std.testing.expectEqual(@as(usize, if (kind == 2) txn_record_external_pinned_size else if (kind == 1) txn_record_external_size else if (modern) txn_record_v7_size else txn_record_v6_size), encoded.len);
        const decoded = try decodeRecord(encoded);
        try std.testing.expectEqual(modern, decoded.document_revisions);
        try std.testing.expectEqualDeep(record.external_decision_owner, decoded.external_decision_owner);
        try std.testing.expectEqualDeep(record.native_pin, decoded.native_pin);
        if (encoded.len > txn_record_v6_size) {
            encoded[txn_record_v6_size] = 2;
            try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(encoded));
        }
    };
}

test "document revision reservation stages the watermark and row custody atomically" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-staged-revision-reservation");
    defer a.free(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const scope: document_revision_reservation.Scope = .{
        .metadata_incarnation = @splat('a'),
        .transaction_id = @splat(1),
        .principal_digest = @splat(2),
        .begin_timestamp = 10,
        .table_id = 3,
        .shard_id = 4,
        .range_id = 5,
    };
    var assigned: u64 = 0;
    for (0..2) |attempt| {
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        try std.testing.expect((try document_revision_reservation.load(&writer, scope.transaction_id)) == null);
        try std.testing.expectError(error.NotFound, writer.get(ttl.document_version_watermark_key));
        var plan = try manager.prepareDocumentRevisionsAtSubtransaction(&writer, scope, &.{"row"}, 20, 100, 7);
        defer plan.deinit();
        assigned = plan.versions[0];
        try plan.stage(&writer);
        if (attempt == 0) writer.abort() else try writer.commit();
    }
    {
        var read = try store.beginProbeTxn();
        defer read.abort();
        const key = try document_revision_reservation.rowKey(a, scope.transaction_id, "row");
        defer a.free(key);
        const bytes = try read.get(key);
        try std.testing.expectEqual(assigned, try document_revision_reservation.rowVersion(bytes));
        try std.testing.expectEqual(@as(u64, 7), try document_revision_reservation.rowSubtransaction(bytes));
        try std.testing.expectEqual(assigned, std.mem.readInt(u64, (try read.get(ttl.document_version_watermark_key))[0..8], .little));
    }
    {
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        var release = try manager.prepareDocumentRevisionRelease(&writer, scope, &.{.{ .key = "row", .version = assigned }});
        defer release.deinit();
        try std.testing.expectEqual(@as(usize, 1), release.released);
        try release.stage(&writer);
        try writer.commit();
    }
    {
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        var next = try manager.prepareDocumentRevisionsAtSubtransaction(&writer, scope, &.{"row"}, 20, 100, 8);
        defer next.deinit();
        try std.testing.expect(next.versions[0] > assigned);
        try next.stage(&writer);
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.prepareDocumentRevisionRelease(&writer, scope, &.{.{ .key = "row", .version = assigned }}));
        try writer.commit();
    }
    {
        var reparent: ?TxnManager.StagedDocumentRevisions = null;
        defer if (reparent) |*plan| plan.deinit();
        var rollback: ?TxnManager.StagedDocumentRevisions = null;
        defer if (rollback) |*plan| plan.deinit();
        var writer = try store.beginWriteTxn();
        defer writer.abort();
        reparent = (try manager.prepareDocumentRevisionSavepoint(&writer, scope.transaction_id, scope.principal_digest, .{ .release_savepoint = .{ .transaction = scope.transaction_id, .first_child = 8, .parent = 7 } })).?;
        try reparent.?.stage(&writer);
        rollback = (try manager.prepareDocumentRevisionSavepoint(&writer, scope.transaction_id, scope.principal_digest, .{ .rollback = .{ .transaction = scope.transaction_id, .first_aborted = 8 } })).?;
        try std.testing.expectEqual(@as(usize, 0), rollback.?.released);
        rollback.?.deinit();
        rollback = null;
        rollback = (try manager.prepareDocumentRevisionSavepoint(&writer, scope.transaction_id, scope.principal_digest, .{ .rollback = .{ .transaction = scope.transaction_id, .first_aborted = 7 } })).?;
        try std.testing.expectEqual(@as(usize, 1), rollback.?.released);
        try rollback.?.stage(&writer);
        try std.testing.expectError(error.NotFound, writer.get(intent_locks_prefix ++ "row"));
        // Aborting this writer also aborts the savepoint metadata and release.
    }
    {
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        try std.testing.expectError(error.LockOwnerChanged, manager.prepareAbandonDocumentRevisions(&writer, scope.transaction_id, @splat(9), 30));
        var close = (try manager.prepareAbandonDocumentRevisions(&writer, scope.transaction_id, scope.principal_digest, 30)).?;
        defer close.deinit();
        try std.testing.expectEqual(@as(usize, 1), close.released);
        try close.stage(&writer);
        try std.testing.expect((try manager.prepareAbandonDocumentRevisions(&writer, scope.transaction_id, scope.principal_digest, 30)) == null);
        try std.testing.expectError(error.NotFound, writer.get(intent_locks_prefix ++ "row"));
        try writer.commit();
    }
}

test "document revision reservation adoption binds the header and exact native decision together" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-adopted-revision-reservation");
    defer a.free(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const scope: document_revision_reservation.Scope = .{
        .metadata_incarnation = @splat('a'),
        .transaction_id = @splat(1),
        .principal_digest = @splat(2),
        .begin_timestamp = 10,
        .table_id = 3,
        .shard_id = 4,
        .range_id = 5,
    };
    {
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        var plan = try manager.prepareDocumentRevisionsAtSubtransaction(&writer, scope, &.{"row"}, 20, 100, 0);
        defer plan.deinit();
        try plan.stage(&writer);
        try writer.commit();
    }
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = scope.metadata_incarnation, .operation_id = @splat(3), .admission_digest = @splat(4), .expires_at_unix_ns = 90 };
    var other = owner;
    other.metadata_incarnation = @splat('b');
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(scope.transaction_id, 10, 30, &.{"owner"}, false, true, .{ .external_decision_owner = other, .document_revision_scope = scope }));
    for (0..2) |_| {
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(scope.transaction_id, 10, 30, &.{"owner"}, false, true, .{ .external_decision_owner = owner, .document_revision_scope = scope });
    }
    const revision_key = try document_revision_reservation.rowKey(a, scope.transaction_id, "row");
    defer a.free(revision_key);
    const assigned = blk: {
        var observation = try store.beginProbeTxn();
        defer observation.abort();
        break :blk try document_revision_reservation.rowVersion(try observation.get(revision_key));
    };
    const intents: []const WriteIntent = &.{.{ .key = "row", .value = "{}" }};
    var extra: MutationExtraBatch = .{ .external_decision_owner = owner, .document_revision_scope = scope, .document_revision_references = &.{.{ .key = "row", .version = assigned + 1 }} };
    try std.testing.expectError(error.VersionConflict, manager.writeIntentsExtraBatch(scope.transaction_id, intents, &.{}, extra));
    extra.document_revision_references = &.{.{ .key = "row", .version = assigned }};
    try manager.writeIntentsExtraBatch(scope.transaction_id, intents, &.{}, extra);
    const intent_key = try manager.makeIntentKey(scope.transaction_id, "row");
    defer a.free(intent_key);
    {
        var read = try store.beginProbeTxn();
        defer read.abort();
        const header = (try document_revision_reservation.load(&read, scope.transaction_id)).?;
        const record = (try externalParticipant(&read, scope.transaction_id)).?;
        try std.testing.expect(header.owner.?.eql(owner));
        try std.testing.expect(record.owner.?.eql(owner));
        try std.testing.expect(record.prepared);
        try std.testing.expect(try document_revision_reservation.rowUsed(try read.get(revision_key)));
        try std.testing.expectEqual(assigned, (try IntentValue.decode(try read.get(intent_key))).document_revision.?);
    }
    {
        var image = try manager.store.beginRead();
        defer image.abort();
        try TxnManager.validateDocumentRevisionSnapshot(a, &image, @import("antfly_cancellation").CancellationToken.none);
    }
    try manager.activateExternalDecision(scope.transaction_id, owner, .{});
    const committed = try manager.resolveIntentsWithExtraBatch(scope.transaction_id, .committed, 50, .{ .external_decision_owner = owner });
    try std.testing.expect(committed.applied);
    try std.testing.expectEqual(@as(?u64, assigned), try ttl.readTimestamp(&store, a, "row"));
    try std.testing.expectEqual(@as(?i128, 50), try ttl.readSourceTimestamp(&store, a, "row"));
    try manager.checkOrdinaryWriteConflicts(&.{"row"});
    const retry = try manager.resolveIntentsWithExtraBatch(scope.transaction_id, .committed, 50, .{ .external_decision_owner = owner });
    try std.testing.expect(!retry.applied);
    {
        var image = try manager.store.beginRead();
        defer image.abort();
        try TxnManager.validateDocumentRevisionSnapshot(a, &image, @import("antfly_cancellation").CancellationToken.none);
    }
}

test "document revision reservation aborts before BEGIN without leaking the reserved row lock" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-prebegin-aborted-revision-reservation");
    defer a.free(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const scope: document_revision_reservation.Scope = .{
        .metadata_incarnation = @splat('a'),
        .transaction_id = @splat(1),
        .principal_digest = @splat(2),
        .begin_timestamp = 10,
        .table_id = 3,
        .shard_id = 4,
        .range_id = 5,
    };
    {
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        var plan = try manager.prepareDocumentRevisionsAtSubtransaction(&writer, scope, &.{"row"}, 20, 100, 0);
        defer plan.deinit();
        try plan.stage(&writer);
        try writer.commit();
    }
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = scope.metadata_incarnation, .operation_id = @splat(3), .admission_digest = @splat(4), .expires_at_unix_ns = 90 };
    const extra: ResolutionExtraBatch = .{ .external_decision_owner = owner, .resolved_participant = "owner" };
    for (0..2) |_| {
        const outcome = try manager.resolveIntentsWithExtraBatch(scope.transaction_id, .aborted, 30, extra);
        try std.testing.expect(!outcome.applied);
    }
    try manager.checkOrdinaryWriteConflicts(&.{"row"});
    var image = try manager.store.beginRead();
    defer image.abort();
    const header = (try document_revision_reservation.load(&image, scope.transaction_id)).?;
    const record = (try externalParticipant(&image, scope.transaction_id)).?;
    try std.testing.expectEqual(TxnStatus.aborted, record.status);
    try std.testing.expect(header.owner.?.eql(owner));
    try std.testing.expectEqual(@as(u32, 0), header.count);
    try TxnManager.validateDocumentRevisionSnapshot(a, &image, @import("antfly_cancellation").CancellationToken.none);
}

test "logical publication journals the actual transaction prepare and roster atomically" {
    try checkPublicationPrepareTerminal(.aborted);
    try checkPublicationPrepareTerminal(.committed);
}

fn checkPublicationPrepareTerminal(comptime status: TxnStatus) !void {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "logical-publication-" ++ @tagName(status));
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(5);
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const Check = struct {
        reject: bool = true,
        calls: usize = 0,
        reject_terminal: bool = true,
        terminal_calls: usize = 0,
        row_key: []const u8,
        const terminal_key = "\x00\x00publication-terminal-test-journal";
        const journal_key = "\x00\x00publication-test-journal";
        fn stage(raw: *anyopaque, view: @import("commit_participant.zig").View, changes: []const @import("publication_prepare_sink.zig").Change) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const record_key = makeRecordKey(id);
            const index_key = makeSidecarKey(participant_index_prefix, id);
            const member_key = makeParticipantIndexKey(participant_index_prefix, id, "owner");
            var record = false;
            var roster = false;
            var member = false;
            for (changes) |change| {
                if (std.mem.eql(u8, change.key, &record_key)) {
                    const decoded = try decodeRecord(change.value.?);
                    try std.testing.expect(decoded.prepared and decoded.publication_capture_required);
                    record = true;
                }
                if (std.mem.eql(u8, change.key, &index_key)) roster = true;
                if (std.mem.eql(u8, change.key, &member_key)) {
                    try std.testing.expectEqualStrings("owner", change.value.?);
                    member = true;
                }
            }
            try std.testing.expect(record and roster and member);
            try std.testing.expectError(error.InvalidCommitMetadata, view.put(&record_key, "corruption"));
            try view.put(journal_key, "captured");
            if (self.reject) return error.TestPublicationRejected;
        }

        fn terminal(raw: *anyopaque, view: @import("commit_participant.zig").View) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.terminal_calls += 1;
            const record = try decodeRecord(try view.get(&makeRecordKey(id)));
            try std.testing.expect(record.status == status and record.intents_resolved and record.publication_capture_required);
            if (status == .committed)
                try std.testing.expectEqualStrings("{}", try view.get(self.row_key))
            else
                try std.testing.expectError(error.NotFound, view.get(self.row_key));
            try view.put(terminal_key, @tagName(status));
            if (self.reject_terminal) return error.TestPublicationRejected;
        }
    };
    const row_key = try internal_keys.documentKeyAlloc(a, "row");
    defer a.free(row_key);
    var check: Check = .{ .row_key = row_key };
    const intents: []const WriteIntent = &.{.{ .key = "row", .value = "{}" }};
    const extra: MutationExtraBatch = .{
        .external_decision_owner = owner,
        .publication_prepare_sink = .{ .ptr = &check, .max_bytes = 64 * 1024, .stage = Check.stage },
    };
    const terminal: ResolutionExtraBatch = .{
        .external_decision_owner = owner,
        .resolved_participant = "owner",
        .publication_terminal_sink = .{ .ptr = &check, .stage = Check.terminal },
    };
    for (0..3) |pass| {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        if (pass == 0) {
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 30, &.{"owner"}, false, true, .{ .external_decision_owner = owner });
            try std.testing.expectError(error.TestPublicationRejected, manager.writeIntentsExtraBatch(id, intents, &.{}, extra));
            try std.testing.expect(!(try manager.loadTransactionRecord(id)).prepared);
            try std.testing.expect(!(try manager.hasIntents(id)));
            try std.testing.expectError(error.NotFound, manager.getAlloc(a, Check.journal_key));
            check.reject = false;
            try manager.writeIntentsExtraBatch(id, intents, &.{}, extra);
        }
        const journal = try manager.getAlloc(a, Check.journal_key);
        defer a.free(journal);
        try std.testing.expectEqualStrings("captured", journal);
        const record = try manager.loadTransactionRecord(id);
        try std.testing.expect(record.prepared and record.publication_capture_required);
        try std.testing.expectEqual(@as(usize, 2), check.calls);
        if (pass == 2) {
            try std.testing.expectEqual(status, record.status);
            try std.testing.expect(!(try manager.hasIntents(id)));
            const terminal_frame = try manager.getAlloc(a, Check.terminal_key);
            defer a.free(terminal_frame);
            try std.testing.expectEqualStrings(@tagName(status), terminal_frame);
            const retry = try manager.resolveIntentsWithExtraBatch(id, status, 50, terminal);
            try std.testing.expect(!retry.applied);
            try std.testing.expectEqual(@as(usize, 2), check.terminal_calls);
            const pending_ack = try manager.getUnresolvedParticipants(a, id);
            defer freeParticipantList(a, pending_ack);
            try std.testing.expectEqual(@as(usize, 1), pending_ack.len);
            try std.testing.expectEqualStrings("owner", pending_ack[0]);
            continue;
        }
        try std.testing.expect(try manager.hasIntents(id));
        try std.testing.expectError(error.LogicalCaptureReceiptMissing, manager.resolveIntentsWithExtraBatch(id, status, 50, .{ .external_decision_owner = owner }));
        try std.testing.expectError(error.PreparedIntentRequiresMaterialization, manager.writeIntentsExtraBatch(id, intents, &.{}, .{ .external_decision_owner = owner }));
        if (pass == 1) {
            if (status == .committed) try manager.activateExternalDecision(id, owner, .{});
            try std.testing.expectError(error.TestPublicationRejected, manager.resolveIntentsWithExtraBatch(id, status, 50, terminal));
            try std.testing.expectEqual(TxnStatus.pending, (try manager.loadTransactionRecord(id)).status);
            try std.testing.expect(try manager.hasIntents(id));
            try std.testing.expectError(error.NotFound, manager.getAlloc(a, Check.terminal_key));
            try std.testing.expectError(error.NotFound, manager.getAlloc(a, row_key));
            check.reject_terminal = false;
            const outcome = try manager.resolveIntentsWithExtraBatch(id, status, 50, terminal);
            try std.testing.expect(outcome.applied);
        }
    }
}

test "Source physical transaction PREPARE atomically retains the exact frozen intents" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "source-frozen-prepare");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const input = source_apply.fixturePreparation();
    const id = input.permit.reference.txn_id;
    const owner = input.permit.decision_owner;
    const realm = @import("db/online_source_contract.zig").namespaceBytes(input.permit.namespace);
    const authority = @import("source_authority.zig");
    const intents: []const WriteIntent = &.{.{ .key = "row", .value = "{\"value\":7}" }};
    const extra: MutationExtraBatch = .{
        .external_decision_owner = owner,
        .replication_prepare = .{ .preparation = input, .position = .{ .raft = .{ .term = 2, .index = 3 } }, .catalog = .{} },
    };
    for (0..4) |pass| {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        if (pass == 0) {
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 30, &.{"owner"}, false, true, .{ .external_decision_owner = owner });
            try std.testing.expectError(error.OnlineSourceScopeChanged, manager.writeIntentsExtraBatch(id, intents, &.{}, extra));
            try std.testing.expect(!(try manager.hasIntents(id)));
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try authority.bind(&txn, .raft, realm);
                try txn.commit();
            }
            try manager.writeIntentsExtraBatch(id, intents, &.{}, extra);
        }
        const record = try manager.loadTransactionRecord(id);
        try std.testing.expect(record.prepared and record.replication_source_required);
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        var read = try store.beginReadTxn();
        defer read.abort();
        const prepared = (try source_apply.readPrepared(arena.allocator(), &read, input.permit)).?;
        try std.testing.expectEqual(@as(u64, 1), prepared.intent_count);
        try std.testing.expectEqual(record.intent_revision, prepared.intent_revision);
        try std.testing.expectEqualDeep(extra.replication_prepare.?.position, prepared.receipt.position);
        if (pass == 0) try std.testing.expectError(error.ReplicationCutoverAuthorityLost, manager.activateExternalDecision(id, owner, .{}));
        if (pass == 1) {
            try manager.activateExternalDecision(id, owner, .{ .replication_activation = .{
                .permit = input.permit,
                .physical_root = input.physical.physical_root_incarnation,
                .position = .{ .raft = .{ .term = 2, .index = 4 } },
            } });
            const resolution: ResolutionExtraBatch = .{
                .external_decision_owner = owner,
                .replication_resolution = .{
                    .permit = input.permit,
                    .physical_root = input.physical.physical_root_incarnation,
                    .position = .{ .raft = .{ .term = 2, .index = 5 } },
                },
            };
            const outcome = try manager.resolveIntentsWithExtraBatch(id, .committed, 40, resolution);
            try std.testing.expect(outcome.applied);
        }
        const ids: []const []const u8 = &.{"owner"};
        const source_ack: source_apply.contract.AckCompletion = .{
            .permit = input.permit,
            .participant_count = 1,
            .participant_ids_digest = try source_apply.contract.participantIdsDigest(ids),
            .participant_id_digest = source_apply.contract.participantIdDigest(ids[0]),
            .decision = .committed,
            .decision_version = 40,
            .terminal_receipts_digest = @splat(1),
        };
        if (pass == 2) {
            var ack: MutationExtraBatch = .{ .external_decision_owner = owner, .replication_ack = .{
                .completion = source_ack,
                .physical_root = input.physical.physical_root_incarnation,
                .position = .{ .raft = .{ .term = 2, .index = 5 } },
            } };
            try std.testing.expectError(error.ReplicationCutoverAuthorityLost, manager.markParticipantsResolvedExtraBatch(id, ids, ack));
            const before = try manager.getResolvedParticipants(a, id);
            defer freeParticipantList(a, before);
            try std.testing.expectEqual(@as(usize, 0), before.len);
            ack.replication_ack.?.position.raft.index += 1;
            try manager.markParticipantsResolvedExtraBatch(id, ids, ack);
            try manager.markParticipantsResolvedExtraBatch(id, ids, ack);
        }
        if (pass == 3) {
            const applied = (try source_apply.readApplied(arena.allocator(), &read, input.permit)).?;
            try std.testing.expectEqual(@as(u64, 5), applied.position.raft.index);
            try std.testing.expectEqual(TxnStatus.committed, record.status);
            const acked = (try source_apply.readAcknowledged(arena.allocator(), &read, source_ack)).?;
            try std.testing.expectEqual(@as(u64, 6), acked.position.raft.index);
            const resolved = try manager.getResolvedParticipants(a, id);
            defer freeParticipantList(a, resolved);
            try std.testing.expectEqualDeep(ids, resolved);
            try std.testing.expect(!(try manager.hasIntents(id)));
        }
    }
}

test "committed history completion admission v3 durably retains every original recovery owner" {
    const history = @import("committed_history.zig");
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const namespace: completion.Namespace = @splat(33);
    {
        var bind = try store.beginWriteTxn();
        errdefer bind.abort();
        try bind.put(&internal_keys.identity_namespace_key, &namespace);
        try @import("source_authority.zig").bind(&bind, .raft, namespace);
        _ = try history.admit(&bind, namespace, @splat(1), 4 * 1024 * 1024, 1000, 1);
        try bind.commit();
    }
    const coordinator: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = namespace, .table_binding = @splat(2), .root_generation = 7, .group_id = 3, .topology_epoch = 4 };
    var follower = coordinator;
    follower.namespace = @splat(34);
    follower.group_id = 5;
    follower.root_generation = 8;
    const members = [_]completion.Participant{
        .{ .namespace = coordinator.namespace, .owner_fence = coordinator.digest(), .roles = completion.Roles.guards, .locator = "coordinator", .input_digest = @splat(3), .routed_owner = coordinator },
        .{ .namespace = follower.namespace, .owner_fence = follower.digest(), .roles = completion.Roles.checkpoint, .locator = "checkpoint", .input_digest = @splat(4), .routed_owner = follower },
    };
    const admission: completion.Admission = .{ .version = 3, .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = namespace, .coordinator_fence = coordinator.digest(), .participants = &members };
    {
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try manager.initTransactionWithCompletionAdmission(admission, 10, 11, true, .{});
    }
    var reopened = try TxnManager.init(a, &store);
    defer reopened.deinit();
    var saved = (try reopened.getCompletionAdmission(a, admission.transaction_id)) orelse return error.TestExpectedRetainedAdmission;
    defer saved.deinit();
    try std.testing.expectEqual(admission.digest(), saved.value.digest);
    try std.testing.expectEqual(coordinator, try saved.value.admission.recoveryOwner(coordinator.namespace));
    try std.testing.expectEqual(follower, try saved.value.admission.recoveryOwner(follower.namespace));
    try reopened.initTransactionWithCompletionAdmission(saved.value.admission, 10, 11, true, .{});
    follower.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, saved.value.admission.requireRoutedOwner(follower));
    try std.testing.expectEqual(TxnStatus.pending, try reopened.getTransactionStatus(admission.transaction_id));
    const key = completion.record_prefix.* ++ admission.transaction_id;
    const pending_raw = try reopened.getAlloc(a, &key);
    defer a.free(pending_raw);
    const pending = try completionCoordinatorRecord(pending_raw);
    try std.testing.expectEqual(.pending, pending.status);
    try std.testing.expect(!pending.prepared and !pending.intents_resolved);
    try std.testing.expectEqual(@as(u64, 0), pending.commit_version);
    _ = try reopened.resolveIntentsWithExtraBatch(admission.transaction_id, .aborted, 30, .{});
    const aborted_raw = try reopened.getAlloc(a, &key);
    defer a.free(aborted_raw);
    const aborted = try completionCoordinatorRecord(aborted_raw);
    try std.testing.expectEqual(.aborted, aborted.status);
    try std.testing.expectEqual(@as(u64, 0), aborted.commit_version);
    try std.testing.expectEqual(@as(u64, 30), aborted.finalized_at_ns);
    try std.testing.expect(aborted.intents_resolved);
}

test "transaction source purpose scoped reference survives restart and fences stale attempts" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-source-scope");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const id: TxnId = @splat(31);
    const purpose: [32]u8 = @splat(32);
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 1 };
    const root = try source_scope.Basis.fromOwner(owner);
    const spec: source_scope.Spec = .{ .actor_table_id = 3, .source_id = @splat(33), .generation = 2, .binding = @splat(34), .checkpoint_owner = root };
    const scope: source_scope.Reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 2, .token = 3, .sequence = 1 };
    const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.effects | completion.Roles.checkpoint, .locator = "docs:0", .input_digest = @splat(35), .routed_owner = owner }};
    const admission: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(36), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
    const extra: MutationExtraBatch = .{ .source_purpose = purpose, .source_scope = scope, .completion_owner = owner, .completion_input_digest = members[0].input_digest };
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var mgr = try TxnManager.init(a, &store);
        defer mgr.deinit();
        {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var bind = try store.beginWriteTxn();
            errdefer bind.abort();
            try bind.put(&internal_keys.identity_namespace_key, &owner.namespace);
            try @import("source_authority.zig").bind(&bind, .native, owner.namespace);
            try bind.commit();
            var enroll = try store.beginWriteTxn();
            errdefer enroll.abort();
            const history = @import("committed_history.zig");
            const consumer = try history.admit(&enroll, owner.namespace, @splat(1), 4 * 1024 * 1024, 1000, 1);
            try enroll.commit();
            var release = try store.beginWriteTxn();
            errdefer release.abort();
            try history.release(&release, owner.namespace, consumer.id, consumer.epoch);
            _ = try source_scope.Store.bind(arena.allocator(), &release, root, spec, .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 10 });
            try release.commit();
        }
        var expired = extra;
        expired.source_scope.?.token = 1;
        var expired_admission = admission;
        expired_admission.transaction_id = @splat(43);
        {
            var fence = try store.beginWriteTxn();
            errdefer fence.abort();
            try std.testing.expectEqual(@as(u64, 1), try fenceUnadmittedSourceAttempt(a, &fence, root, expired.source_scope.?, expired_admission.transaction_id));
            try std.testing.expectEqual(@as(u64, 1), try fenceUnadmittedSourceAttempt(a, &fence, root, expired.source_scope.?, expired_admission.transaction_id));
            try fence.commit();
        }
        try std.testing.expectError(error.SourceCheckpointConflict, mgr.initTransactionWithCompletionAdmission(expired_admission, 10_000, 1_000, true, expired));
        try std.testing.expectError(error.TxnNotFound, mgr.getTransactionStatus(expired_admission.transaction_id));
        var legacy_admission = admission;
        legacy_admission.transaction_id = @splat(40);
        try std.testing.expectError(error.SourceCheckpointConflict, mgr.initTransactionWithCompletionAdmission(legacy_admission, 10_000, 1_000, true, .{ .source_purpose = purpose }));
        try std.testing.expectError(error.TxnNotFound, mgr.getTransactionStatus(legacy_admission.transaction_id));
        var unbound_admission = admission;
        unbound_admission.transaction_id = @splat(41);
        var unbound_extra = extra;
        unbound_extra.source_scope.?.domain = @splat(42);
        try std.testing.expectError(error.SourceCheckpointConflict, mgr.initTransactionWithCompletionAdmission(unbound_admission, 10_000, 1_000, true, unbound_extra));
        try std.testing.expectError(error.TxnNotFound, mgr.getTransactionStatus(unbound_admission.transaction_id));
        try std.testing.expectError(error.NotFound, store.get(a, &makeSidecarKey(completion_admission_prefix, unbound_admission.transaction_id)));
        try mgr.initTransactionWithCompletionAdmission(admission, 10_000, 1_000, true, extra);
        {
            var fence = try store.beginWriteTxn();
            defer fence.abort();
            try std.testing.expectError(error.SourceOutcomeUnresolved, fenceUnadmittedSourceAttempt(a, &fence, root, scope, id));
            try std.testing.expectError(error.SourceOutcomeUnresolved, fenceSettledSourceAttempt(a, &fence, root, scope, id));
        }
        try mgr.initTransactionWithCompletionAdmission(admission, 10_000, 2_000, true, extra);
        try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.initTransactionWithCompletionAdmission(admission, 10_000, 1_000, true, .{ .source_purpose = purpose }));
        var changed = extra;
        changed.source_scope.?.token += 1;
        try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.initTransactionWithCompletionAdmission(admission, 10_000, 1_000, true, changed));
        try mgr.writeIntentsExtraBatch(id, &.{.{ .key = "a", .value = "scoped" }}, &.{}, extra);
        try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.writeIntentsExtraBatch(id, &.{.{ .key = "a", .value = "stale" }}, &.{}, changed));
        const raw = try mgr.getAlloc(a, &makeRecordKey(id));
        defer a.free(raw);
        try std.testing.expectEqual(@as(usize, txn_record_v9_size), raw.len);
        try std.testing.expectEqualDeep(scope, (try decodeRecord(raw)).source_scope.?);
        try std.testing.expectEqualDeep(scope, (try completionCoordinatorRecord(raw)).source_scope.?);
        const corrupt = try a.dupe(u8, raw);
        defer a.free(corrupt);
        corrupt[54] = 1;
        try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(corrupt));
        @memcpy(corrupt, raw);
        @memset(corrupt[87..119], 0);
        try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(corrupt));
        @memcpy(corrupt, raw);
        corrupt[51] = 0;
        try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(corrupt));
    }
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var mgr = try TxnManager.init(a, &store);
        defer mgr.deinit();
        const summaries = try mgr.listTransactions(a);
        defer a.free(summaries);
        try std.testing.expectEqualDeep(scope, summaries[0].source_scope.?);
        try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.resolveIntentsWithExtraBatch(id, .aborted, 11_000, .{ .source_purpose = purpose }));
        var changed = scope;
        changed.generation += 1;
        try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.resolveIntentsWithExtraBatch(id, .aborted, 11_000, .{ .source_purpose = purpose, .source_scope = changed }));
        {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var closing = try store.beginWriteTxn();
            errdefer closing.abort();
            const closed = try source_scope.Store.close(arena.allocator(), &closing, root, spec.domain(), spec, .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 11 }, .{ .term = 1, .index = 20 });
            try std.testing.expectEqual(source_scope.Phase.closing, closed.phase);
            try closing.commit();
        }
        // An already durable prepare remains idempotent, but no new decision
        // can turn this closing coordinator into a committed source attempt.
        try mgr.writeIntentsExtraBatch(id, &.{.{ .key = "a", .value = "scoped" }}, &.{}, extra);
        try std.testing.expectError(error.SourceCheckpointConflict, mgr.resolveIntentsWithExtraBatch(id, .committed, 11_000, .{ .source_purpose = purpose, .source_scope = scope, .completion_owner = owner, .source_scope_cut = .{ .term = 1, .index = 21 } }));
        try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(id));
        try std.testing.expect(try mgr.hasIntents(id));
        try std.testing.expectError(error.InvalidSourceAdmission, mgr.resolveIntentsWithExtraBatch(id, .aborted, 11_000, .{ .source_purpose = purpose, .source_scope = scope, .completion_owner = owner }));
        try std.testing.expectEqual(TxnStatus.pending, try mgr.getTransactionStatus(id));
        _ = try mgr.resolveIntentsWithExtraBatch(id, .aborted, 11_000, .{ .source_purpose = purpose, .source_scope = scope, .completion_owner = owner, .source_scope_cut = .{ .term = 1, .index = 21 } });
        {
            var probe = try store.beginReadTxn();
            defer probe.abort();
            const closed = (try source_scope.Store.readRecord(a, &probe, spec.domain())).?;
            try std.testing.expectEqual(source_scope.Phase.closed, closed.phase);
            try std.testing.expect(closed.slot == null);
            try std.testing.expectEqual(scope.token, closed.closed_through_token);
            try std.testing.expectEqualDeep(source_scope.Cut{ .term = 1, .index = 21 }, closed.closed_at.?);
        }

        try std.testing.expectError(error.SourceTransactionIdentityConflict, mgr.resolveIntentsWithExtraBatch(id, .aborted, 11_000, .{}));
        const raw = try mgr.getAlloc(a, &makeRecordKey(id));
        defer a.free(raw);
        try std.testing.expectEqualDeep(scope, (try decodeRecord(raw)).source_scope.?);
        try std.testing.expectEqual(TxnStatus.aborted, (try decodeRecord(raw)).status);
        {
            var probe = try store.beginWriteTxn();
            defer probe.abort();
            try std.testing.expectError(error.SourceOutcomeUnresolved, fenceSettledSourceAttempt(a, &probe, root, scope, id));
        }
        try mgr.markParticipantResolved(id, members[0].locator);
        {
            var proof = try store.beginWriteTxn();
            defer proof.abort();
            try std.testing.expectError(error.SourceOutcomeUnresolved, fenceUnadmittedSourceAttempt(a, &proof, root, scope, id));
            try std.testing.expectEqual(scope.token, try fenceSettledSourceAttempt(a, &proof, root, scope, id));
        }
        var other_spec = spec;
        other_spec.source_id = @splat(45);
        other_spec.binding = @splat(46);
        var other_admission = admission;
        other_admission.transaction_id = @splat(44);
        var other_extra = extra;
        other_extra.source_scope.?.domain = other_spec.domain();
        other_extra.source_scope.?.binding = other_spec.binding;
        other_extra.source_scope.?.token = 1;
        {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var bind = try store.beginWriteTxn();
            errdefer bind.abort();
            _ = try source_scope.Store.bind(arena.allocator(), &bind, root, other_spec, .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 12 });
            try bind.commit();
        }
        try mgr.initTransactionWithCompletionAdmission(other_admission, 12_000, 2_000, true, other_extra);
        {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var closing = try store.beginWriteTxn();
            errdefer closing.abort();
            _ = try source_scope.Store.close(arena.allocator(), &closing, root, other_spec.domain(), other_spec, .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 13 }, .{ .term = 1, .index = 23 });
            try closing.commit();
        }
        try std.testing.expectError(error.SourceCheckpointConflict, mgr.writeIntentsExtraBatch(other_admission.transaction_id, &.{.{ .key = "other", .value = "blocked" }}, &.{}, other_extra));
        try std.testing.expect(!(try mgr.loadTransactionRecord(other_admission.transaction_id)).prepared);
        try std.testing.expect(!try mgr.hasIntents(other_admission.transaction_id));
        _ = try mgr.resolveIntentsWithExtraBatch(other_admission.transaction_id, .aborted, 13_000, .{ .source_purpose = purpose, .source_scope = other_extra.source_scope, .completion_owner = owner, .source_scope_cut = .{ .term = 1, .index = 24 } });
    }
}

test "transaction source purpose retirement fences delayed BEGIN across real store reopen" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-source-retirement");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    try @import("source_scope_retirement_native_test.zig").run(a, path);
}

test "transaction source purpose ordinary provenance is explicit immutable and rejects corrupt encodings" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-ordinary-producer");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    const id: TxnId = @splat(240);
    const extra: MutationExtraBatch = .{ .ordinary_producer = true };
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(id, 10, 11, &.{}, true, true, extra);
    const raw = try manager.getAlloc(a, &makeRecordKey(id));
    defer a.free(raw);
    try std.testing.expectEqual(@as(usize, txn_record_v10_size), raw.len);
    try std.testing.expect((try decodeRecord(raw)).ordinary_producer);
    try std.testing.expect(!(try decodeRecord(raw[0..txn_record_v6_size])).ordinary_producer);
    const corrupt = try a.dupe(u8, raw);
    defer a.free(corrupt);
    corrupt[54] = 0;
    try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(corrupt));
    corrupt[54] = 2;
    try std.testing.expect((try decodeRecord(corrupt)).legacy_producer);
    try std.testing.expect(!(try decodeRecord(corrupt)).ordinary_producer);
    corrupt[54] = 3;
    try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(corrupt));
    corrupt[54] = 1;
    corrupt[53] = 2;
    try std.testing.expectError(error.InvalidTxnRecord, decodeRecord(corrupt));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(id, 10, 11, &.{}, true, true));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.writeIntentsExtraBatch(id, &.{}, &.{}, .{}));
    try manager.writeIntentsExtraBatch(id, &.{}, &.{}, extra);
    _ = try manager.resolveIntentsWithExtraBatch(id, .aborted, 20, .{});
    try std.testing.expect((try manager.loadTransactionRecord(id)).ordinary_producer);
    const legacy: TxnId = @splat(241);
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(legacy, 10, 11, &.{}, true, true);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(legacy, 10, 11, &.{}, true, true, extra));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(@splat(242), 10, 11, &.{}, true, true, .{ .ordinary_producer = true, .source_purpose = @splat(1) }));
}

test "transaction source purpose closed legacy recovery inventory is bounded and survives reopen" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-legacy-inventory");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 1 };
    const root = try source_scope.Basis.fromOwner(owner);
    const policy = @import("source_producer_policy.zig");
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        try std.testing.expectError(error.SourcePendingWork, manager.legacyRecoveryPage(a, root, null, 1));
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(@splat(1), 10, 11, &.{}, true, true, .{ .ordinary_producer = true });
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(@splat(2), 10, 11, &.{}, true, true, .{ .legacy_producer = true });
        try manager.writeIntentsExtraBatch(@splat(2), &.{.{ .key = "legacy-row", .value = "{}" }}, &.{}, .{ .legacy_producer = true });
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(@splat(3), 10, 11, &.{}, true, true);
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(@splat(4), 10, 11, &.{}, true, true);
        _ = try manager.resolveIntentsWithExtraBatch(@splat(4), .aborted, 20, .{});
        // Historic terminal records without a resolution bit, and terminal
        // decisions with unresolved intents, must both remain discoverable.
        const terminal = try manager.getAlloc(a, &makeRecordKey(@splat(4)));
        defer a.free(terminal);
        try manager.putValue(&makeRecordKey(@splat(5)), terminal[0..txn_record_v5_size]);
        const unresolved = try a.dupe(u8, terminal);
        defer a.free(unresolved);
        unresolved[52] = 0;
        try manager.putValue(&makeRecordKey(@splat(6)), unresolved);
        var batch = try store.beginWriteTxn();
        errdefer batch.abort();
        _ = try policy.Store.close(a, &batch, root, .{ .term = 1, .index = 7 });
        try batch.commit();
    }
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    try std.testing.expectError(error.InvalidSourceAdmission, manager.legacyRecoveryPage(a, root, null, 0));
    try std.testing.expectError(error.InvalidSourceAdmission, manager.legacyRecoveryPage(a, root, null, 257));
    var wrong = root;
    wrong.root_generation += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, manager.legacyRecoveryPage(a, wrong, null, 1));
    var first = try manager.legacyRecoveryPage(a, root, null, 1);
    defer first.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), first.items.len);
    try std.testing.expectEqual(@as(usize, 1), first.scanned_records);
    try std.testing.expect(!first.reached_end);
    try std.testing.expectEqualDeep(@as(TxnId, @splat(1)), first.next_after.?);
    var second = try manager.legacyRecoveryPage(a, root, first.next_after, 1);
    defer second.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), second.items.len);
    try std.testing.expectEqualDeep(@as(TxnId, @splat(2)), second.items[0].transaction_id);
    try std.testing.expect(second.items[0].prepared);
    const raw = try manager.getAlloc(a, &makeRecordKey(@splat(2)));
    defer a.free(raw);
    try std.testing.expectEqualDeep(completion.bytesDigest(raw), second.items[0].record_digest);
    var third = try manager.legacyRecoveryPage(a, root, second.next_after, 256);
    defer third.deinit(a);
    try std.testing.expect(third.reached_end);
    try std.testing.expectEqual(@as(usize, 4), third.scanned_records);
    try std.testing.expectEqual(@as(usize, 3), third.items.len);
    try std.testing.expectEqualDeep(@as(TxnId, @splat(3)), third.items[0].transaction_id);
    try std.testing.expectEqualDeep(@as(TxnId, @splat(5)), third.items[1].transaction_id);
    try std.testing.expect(!third.items[1].intents_resolved_known);
    try std.testing.expectEqualDeep(@as(TxnId, @splat(6)), third.items[2].transaction_id);
    try std.testing.expect(third.items[2].intents_resolved_known and !third.items[2].intents_resolved);
    // Discovery cannot authorize either terminal decision through closed ingress.
    try std.testing.expectError(error.SourcePendingWork, manager.resolveIntentsWithExtraBatch(@splat(2), .aborted, 30, .{}));
    try std.testing.expect(try manager.hasIntents(@splat(2)));
    var all = try manager.legacyRecoveryPage(a, root, null, 256);
    defer all.deinit(a);
    try std.testing.expectEqual(@as(usize, 4), all.items.len);
    try std.testing.expectEqual(@as(u64, 7), all.closure.first_closed_at.index);
    try std.testing.expectError(error.InvalidTxnRecord, manager.putValue(&makeRecordKey(@splat(3)), "corrupt"));
    try store.put(&makeRecordKey(@splat(3)), "corrupt");
    try std.testing.expectError(error.InvalidTxnRecord, manager.legacyRecoveryPage(a, root, null, 256));
    try store.put(&makeRecordKey(@splat(3)), raw);
    // A malformed short key sorts before every full transaction ID. The
    // initial cursor must inspect the prefix itself rather than prefix+zero.
    try manager.putValue(records_prefix, raw);
    try std.testing.expectError(error.InvalidTxnRecord, manager.legacyRecoveryPage(a, root, null, 256));
}

test "transaction source purpose strict binding proves legacy drainage in its writer" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-legacy-drained");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 1 };
    const root = try source_scope.Basis.fromOwner(owner);
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(@splat(1), 10, 11, &.{}, true, true, .{ .ordinary_producer = true });
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(@splat(2), 10, 11, &.{}, true, true);
    _ = try manager.resolveIntentsWithExtraBatch(@splat(2), .aborted, 20, .{});
    const terminal = try manager.getAlloc(a, &makeRecordKey(@splat(2)));
    defer a.free(terminal);
    var writer = try store.beginWriteTxn();
    defer writer.abort();
    try std.testing.expectError(error.SourcePendingWork, TxnManager.requireLegacyDrainedInWriter(a, &writer, root));
    _ = try @import("source_producer_policy.zig").Store.close(a, &writer, root, .{ .term = 1, .index = 7 });
    try TxnManager.requireLegacyDrainedInWriter(a, &writer, root);
    try writer.put(&makeRecordKey(@splat(3)), terminal[0..txn_record_v5_size]);
    try std.testing.expectError(error.SourcePendingWork, TxnManager.requireLegacyDrainedInWriter(a, &writer, root));
    try writer.delete(&makeRecordKey(@splat(3)));
    try writer.put(records_prefix, terminal);
    try std.testing.expectError(error.InvalidTxnRecord, TxnManager.requireLegacyDrainedInWriter(a, &writer, root));
}

test "transaction source purpose closed legacy terminal recovery preserves original decisions and effects" {
    const a = std.testing.allocator;
    const path = try tempTestPath(a, "txn-legacy-terminal");
    defer a.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 1 };
    const root = try source_scope.Basis.fromOwner(owner);
    const policy = @import("source_producer_policy.zig");
    {
        var store = try DocStore.open(a, path, .{});
        defer store.close();
        var manager = try TxnManager.init(a, &store);
        defer manager.deinit();
        for ([_]u8{ 1, 2, 3 }) |byte| {
            const id: TxnId = @splat(byte);
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(id, 10, 11, &.{}, true, true);
            if (byte == 1) continue;
            try manager.writeIntents(id, &.{.{ .key = if (byte == 2) "commit-row" else "abort-row", .value = "original" }}, &.{});
            // Fixture models the historic crash window: original decision
            // persisted, prepared intents not yet retired.
            var record = try manager.loadTransactionRecord(id);
            record.status = if (byte == 2) .committed else .aborted;
            record.commit_version = if (byte == 2) 30 else 0;
            record.finalized_at = 31;
            const encoded = try manager.encodeRecord(record);
            defer a.free(encoded);
            try manager.putValue(&makeRecordKey(id), if (byte == 3) encoded[0..txn_record_v5_size] else encoded);
        }
        var batch = try store.beginWriteTxn();
        errdefer batch.abort();
        _ = try policy.Store.close(a, &batch, root, .{ .term = 1, .index = 7 });
        try batch.commit();
    }
    var store = try DocStore.open(a, path, .{});
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    var page = try manager.legacyRecoveryPage(a, root, null, 256);
    defer page.deinit(a);
    try std.testing.expectEqual(@as(usize, 3), page.items.len);
    try std.testing.expect(!page.items[2].intents_resolved_known);
    const pending: LegacyTerminalResolution = .{ .root = root, .transaction_id = page.items[0].transaction_id, .record_digest = page.items[0].record_digest };
    try std.testing.expectError(error.SourceOutcomeUnresolved, manager.resolveClosedLegacyTerminal(pending, .{}));
    var commit: LegacyTerminalResolution = .{ .root = root, .transaction_id = page.items[1].transaction_id, .record_digest = page.items[1].record_digest };
    var absent = commit;
    absent.transaction_id = @splat(239);
    try std.testing.expectError(error.TxnNotFound, manager.resolveClosedLegacyTerminal(absent, .{}));
    var forged = commit;
    forged.record_digest[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.resolveClosedLegacyTerminal(forged, .{}));
    forged = commit;
    forged.root.root_generation += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, manager.resolveClosedLegacyTerminal(forged, .{}));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.resolveIntentsWithExtraBatchStagedAndLegacy(commit.transaction_id, .committed, 30, .{}, forged));
    forged = commit;
    forged.record_digest[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.resolveIntentsWithExtraBatchStagedAndLegacy(commit.transaction_id, .committed, 30, .{}, forged));
    try std.testing.expectError(error.DecisionConflict, manager.resolveIntentsWithExtraBatchStagedAndLegacy(commit.transaction_id, .aborted, 99, .{}, commit));
    try std.testing.expectError(error.SourcePendingWork, manager.resolveIntentsWithExtraBatch(commit.transaction_id, .committed, 30, .{}));
    try std.testing.expect(try manager.hasIntents(commit.transaction_id));
    _ = try manager.resolveClosedLegacyTerminal(commit, .{});
    const commit_key = try internal_keys.documentKeyAlloc(a, "commit-row");
    defer a.free(commit_key);
    const abort_key = try internal_keys.documentKeyAlloc(a, "abort-row");
    defer a.free(abort_key);
    const value = try manager.getAlloc(a, commit_key);
    defer a.free(value);
    try std.testing.expectEqualStrings("original", value);
    const resolved = try manager.loadTransactionRecord(commit.transaction_id);
    try std.testing.expectEqual(TxnStatus.committed, resolved.status);
    try std.testing.expectEqual(@as(u64, 30), resolved.commit_version);
    try std.testing.expectEqual(@as(u64, 31), resolved.finalized_at);
    try std.testing.expect(resolved.intents_resolved and !try manager.hasIntents(commit.transaction_id));
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.resolveClosedLegacyTerminal(commit, .{}));
    // Re-read current state for an explicit idempotent terminal retry. It must
    // not reapply the original row over a newer value.
    const resolved_bytes = try manager.getAlloc(a, &makeRecordKey(commit.transaction_id));
    defer a.free(resolved_bytes);
    const metadata_marker = "\x00\x00__metadata__:legacy_terminal_atomic_test";
    try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.applyBatchStagedWithHistory(&.{.{ .key = metadata_marker, .value = "stale" }}, &.{}, null, null, null, null, null, commit));
    try std.testing.expectError(error.NotFound, manager.getAlloc(a, metadata_marker));
    try std.testing.expectError(error.TxnNotFound, manager.applyBatchStagedWithHistory(&.{.{ .key = metadata_marker, .value = "absent" }}, &.{}, null, null, null, null, null, absent));
    try std.testing.expectError(error.NotFound, manager.getAlloc(a, metadata_marker));
    commit.record_digest = completion.bytesDigest(resolved_bytes);
    try manager.putValue(commit_key, "newer");
    try std.testing.expect(!(try manager.resolveClosedLegacyTerminal(commit, .{ .completion_writes = &.{.{ .key = metadata_marker, .value = "current" }} })).applied);
    const marked = try manager.getAlloc(a, metadata_marker);
    defer a.free(marked);
    try std.testing.expectEqualStrings("current", marked);
    const newer = try manager.getAlloc(a, commit_key);
    defer a.free(newer);
    try std.testing.expectEqualStrings("newer", newer);
    const abort: LegacyTerminalResolution = .{ .root = root, .transaction_id = page.items[2].transaction_id, .record_digest = page.items[2].record_digest };
    _ = try manager.resolveClosedLegacyTerminal(abort, .{});
    try std.testing.expectError(error.NotFound, manager.getAlloc(a, abort_key));
    try std.testing.expect(!try manager.hasIntents(abort.transaction_id));
    var remaining = try manager.legacyRecoveryPage(a, root, null, 256);
    defer remaining.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), remaining.items.len);
    try std.testing.expectEqualDeep(pending.transaction_id, remaining.items[0].transaction_id);
}
