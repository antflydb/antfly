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

//! Progress v2 is the sole durable record of a consumer's replay progress.
//! There used to be a second, older representation here (a scalar KV key, a
//! richer "enriched" KV key, and an optional file sidecar, each written by
//! whichever caller happened to touch an index) that this file deliberately
//! no longer has: Antfly is unreleased, so there was no need to keep it
//! around for compatibility, and splitting one consumer's progress across
//! three storage locations was exactly the layering mistake WS6 exists to
//! remove. Every reader and writer in this tree now goes through
//! `contributeProgress`/`loadProgress` (or the `Txn` variants) and the
//! single key `\x00\x00__metadata__:progress:v2:<consumer>`.

const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const docstore_mod = @import("../../docstore.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const artifact_projection_epoch = @import("../artifact_projection_epoch.zig");
const artifact_publication = @import("../artifact_publication.zig");

const partial_cursor_prefix = "\x00\x00__metadata__:derived_partial:";

/// Resume point inside one oversized replay record: the items of `sequence`
/// before `offset` are already applied and durable in the index. Valid only
/// for the index configuration it was written under, and only while
/// `sequence` is the next record to apply; once that sequence completes, the
/// ordinary applied sequence passes it and the cursor is ignored.
pub const PartialReplayCursor = struct {
    sequence: u64,
    offset: u64,
    config_hash: u64,
};

const partial_cursor_value_len = 3 * @sizeOf(u64);

pub fn savePartialReplayCursor(alloc: Allocator, store: anytype, index_name: []const u8, cursor: PartialReplayCursor) !void {
    var key_buf: [256]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}{s}", .{ partial_cursor_prefix, index_name });
    var value: [partial_cursor_value_len]u8 = undefined;
    std.mem.writeInt(u64, value[0..8], cursor.sequence, .little);
    std.mem.writeInt(u64, value[8..16], cursor.offset, .little);
    std.mem.writeInt(u64, value[16..24], cursor.config_hash, .little);
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &value);
    try txn.commit();
}

/// Removes an index's resume point. Every reset of its applied progress
/// (drop, recreate, rebuild, recovery replacement) must also clear it: a
/// recreated index replaying the same record must not skip items that only
/// the old index applied.
pub fn clearPartialReplayCursor(alloc: Allocator, store: anytype, index_name: []const u8) !void {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try clearPartialReplayCursorTxn(&txn, index_name);
    try txn.commit();
}

fn clearPartialReplayCursorTxn(txn: anytype, index_name: []const u8) !void {
    var key_buf: [256]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}{s}", .{ partial_cursor_prefix, index_name });
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

pub fn loadPartialReplayCursor(alloc: Allocator, store: anytype, index_name: []const u8) !?PartialReplayCursor {
    var key_buf: [256]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}{s}", .{ partial_cursor_prefix, index_name });
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != partial_cursor_value_len) return error.InvalidDerivedApplyState;
    return .{
        .sequence = std.mem.readInt(u64, raw[0..8], .little),
        .offset = std.mem.readInt(u64, raw[8..16], .little),
        .config_hash = std.mem.readInt(u64, raw[16..24], .little),
    };
}

pub const ProjectionStatus = enum(u8) {
    clean = 1,
    rebuilding = 2,
    degraded = 3,
    repair_required = 4,
};

/// In-memory view of one consumer's identity/lifecycle fields, independent
/// of whatever encoding backs it. Every reader/writer below takes or
/// returns this shape; only `encodeProgressRecord`/`decodeProgressRecord`
/// know about the actual v2 wire format.
pub const ProjectionCheckpoint = struct {
    applied_sequence: u64 = 0,
    status: ProjectionStatus = .clean,
    generation: u64 = 0,
    config_hash: u64 = 0,
    /// Physical result cardinality certified by the same durable publication
    /// boundary as `applied_sequence`. This is deliberately distinct from the
    /// live artifact target: generated artifacts may advance while this last
    /// atomically published snapshot remains safe to query.
    published_count: ?u64 = null,
};

pub const AppliedSequenceUpdate = struct {
    index_name: []const u8,
    sequence: u64,
    status: ProjectionStatus = .clean,
    generation: u64 = 0,
    config_hash: u64 = 0,
    published_count: ?u64 = null,

    fn checkpoint(self: @This()) ProjectionCheckpoint {
        return .{
            .applied_sequence = self.sequence,
            .status = self.status,
            .generation = self.generation,
            .config_hash = self.config_hash,
            .published_count = self.published_count,
        };
    }
};

// --- Progress v2: one authoritative record per consumer -------------------
//
// One key per consumer merges the applied sequence, the projection seal/
// identity fields and the LSM WAL checkpoint boundary
// (`checkpointLsmWalForManagedIndex`'s physical WAL-truncation marker lives
// on the per-backend pager and cannot be fetched back from a KV read; this
// field records only whether the docstore-side watermark was advanced after
// that WAL boundary was confirmed).
const progress_prefix_v2 = "\x00\x00__metadata__:progress:v2:";
const progress_magic_v2 = "AFPROG2V";
const progress_encoded_len_v2 = progress_magic_v2.len + 8 + 1 + 8 + 8 + 1 + 8 + 8 + 4;

/// The merged, authoritative progress identity for one consumer: the applied
/// sequence, the projection seal/identity fields (status/generation/
/// config_hash/published_count), and the sequence boundary through which the
/// consumer's LSM WAL has been checkpointed.
pub const ProgressRecord = struct {
    applied_sequence: u64 = 0,
    status: ProjectionStatus = .clean,
    generation: u64 = 0,
    config_hash: u64 = 0,
    published_count: ?u64 = null,
    lsm_wal_checkpointed_through: u64 = 0,

    fn checkpoint(self: @This()) ProjectionCheckpoint {
        return .{
            .applied_sequence = self.applied_sequence,
            .status = self.status,
            .generation = self.generation,
            .config_hash = self.config_hash,
            .published_count = self.published_count,
        };
    }

    fn withCheckpoint(self: @This(), value: ProjectionCheckpoint) @This() {
        var next = self;
        next.applied_sequence = value.applied_sequence;
        next.status = value.status;
        next.generation = value.generation;
        next.config_hash = value.config_hash;
        next.published_count = value.published_count;
        return next;
    }
};

/// One consumer's contribution to a single progress commit. `consumer` is
/// typically an index name; callers that batch several consumers into one
/// publication (one native commit, under WS4) pass several `Progress`
/// values to one `contributeProgress`/`contributeProgressTxn` call so the
/// record for each is written inside the same transaction.
pub const Progress = struct {
    consumer: []const u8,
    sequence: u64,
    status: ProjectionStatus = .clean,
    generation: u64 = 0,
    config_hash: u64 = 0,
    published_count: ?u64 = null,
    lsm_wal_checkpointed_through: u64 = 0,
};

/// A raw key/value pair to piggyback into the same progress commit (used by
/// callers to fold a derived status snapshot write into this transaction
/// instead of committing it separately). The value must outlive the call.
pub const StatusWrite = struct {
    key: []const u8,
    value: []const u8,
};

var progress_commit_count: std.atomic.Value(u64) = .init(0);

/// Count of completed `contributeProgress` commits. Test-only instrumentation:
/// production code has no reason to read this, since the whole point of the
/// counter is to let a test assert "one publication, one progress commit"
/// without threading a mock transaction through the real write path.
pub fn progressCommitCount() u64 {
    return progress_commit_count.load(.monotonic);
}

pub fn resetProgressCommitCountForTest() void {
    progress_commit_count.store(0, .monotonic);
}

fn progressKeyV2(buffer: []u8, consumer: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{s}", .{ progress_prefix_v2, consumer });
}

fn encodeProgressRecord(record: ProgressRecord) [progress_encoded_len_v2]u8 {
    var raw: [progress_encoded_len_v2]u8 = undefined;
    var pos: usize = 0;
    @memcpy(raw[0..progress_magic_v2.len], progress_magic_v2);
    pos += progress_magic_v2.len;
    writeProgressInt(&raw, &pos, u64, record.applied_sequence);
    writeProgressInt(&raw, &pos, u8, @backingInt(record.status));
    writeProgressInt(&raw, &pos, u64, record.generation);
    writeProgressInt(&raw, &pos, u64, record.config_hash);
    writeProgressInt(&raw, &pos, u8, @intFromBool(record.published_count != null));
    writeProgressInt(&raw, &pos, u64, record.published_count orelse 0);
    writeProgressInt(&raw, &pos, u64, record.lsm_wal_checkpointed_through);
    writeProgressInt(&raw, &pos, u32, Crc32.hash(raw[0 .. progress_encoded_len_v2 - 4]));
    return raw;
}

fn decodeProgressRecord(raw: []const u8) !ProgressRecord {
    if (raw.len != progress_encoded_len_v2) return error.InvalidDerivedApplyState;
    if (!std.mem.eql(u8, raw[0..progress_magic_v2.len], progress_magic_v2)) return error.InvalidDerivedApplyState;
    if (Crc32.hash(raw[0 .. progress_encoded_len_v2 - 4]) != std.mem.readInt(u32, raw[progress_encoded_len_v2 - 4 ..][0..4], .little))
        return error.InvalidDerivedApplyState;
    var pos: usize = progress_magic_v2.len;
    const applied_sequence = readProgressInt(raw, &pos, u64);
    const status_raw = readProgressInt(raw, &pos, u8);
    const generation = readProgressInt(raw, &pos, u64);
    const config_hash = readProgressInt(raw, &pos, u64);
    const published_count_present = readProgressInt(raw, &pos, u8);
    if (published_count_present > 1) return error.InvalidDerivedApplyState;
    const published_count_raw = readProgressInt(raw, &pos, u64);
    const lsm_wal_checkpointed_through = readProgressInt(raw, &pos, u64);
    return .{
        .applied_sequence = applied_sequence,
        .status = std.enums.fromInt(ProjectionStatus, status_raw) orelse return error.InvalidDerivedApplyState,
        .generation = generation,
        .config_hash = config_hash,
        .published_count = if (published_count_present == 1) published_count_raw else null,
        .lsm_wal_checkpointed_through = lsm_wal_checkpointed_through,
    };
}

fn writeProgressInt(raw: []u8, pos: *usize, comptime T: type, value: T) void {
    const size = @sizeOf(T);
    std.mem.writeInt(T, raw[pos.* .. pos.* + size][0..size], value, .little);
    pos.* += size;
}

fn readProgressInt(raw: []const u8, pos: *usize, comptime T: type) T {
    const size = @sizeOf(T);
    const out = std.mem.readInt(T, raw[pos.* .. pos.* + size][0..size], .little);
    pos.* += size;
    return out;
}

/// Write one consumer's v2 progress entry into an already-open write
/// transaction/write-batch. The caller owns `commit()`. This is the hook a
/// future WS4 `CommitPlan` calls directly once it exposes the same
/// `put`-shaped surface as the rest of this file's `txn: anytype` helpers
/// (see `commit_participant.View`) — `contributeProgress` below is a thin
/// convenience wrapper around exactly this for callers that do not yet have
/// an open plan/batch of their own.
pub fn contributeProgressTxn(txn: anytype, progress: Progress) !void {
    var key_buf: [256]u8 = undefined;
    const key = try progressKeyV2(&key_buf, progress.consumer);
    const encoded = encodeProgressRecord(.{
        .applied_sequence = progress.sequence,
        .status = progress.status,
        .generation = progress.generation,
        .config_hash = progress.config_hash,
        .published_count = progress.published_count,
        .lsm_wal_checkpointed_through = progress.lsm_wal_checkpointed_through,
    });
    var mutable_txn = txn;
    try mutable_txn.put(key, &encoded);
}

/// The one authoritative progress commit for a publication: every consumer
/// it touches, plus any status snapshot the caller chooses to piggyback,
/// lands in exactly one `txn.commit()`. Call this once per publication,
/// not once per consumer — batch every `Progress` value touched by that
/// publication into one call so `progressCommitCount()` advances by exactly
/// one regardless of how many consumers were updated.
pub fn contributeProgress(alloc: Allocator, store: anytype, updates: []const Progress, status_writes: []const StatusWrite) !void {
    if (updates.len == 0 and status_writes.len == 0) return;
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    for (updates) |progress| try contributeProgressTxn(&txn, progress);
    for (status_writes) |write| try txn.put(write.key, write.value);
    try txn.commit();
    _ = progress_commit_count.fetchAdd(1, .monotonic);
}

pub fn loadProgressTxn(txn: anytype, consumer: []const u8) !?ProgressRecord {
    var key_buf: [256]u8 = undefined;
    const key = try progressKeyV2(&key_buf, consumer);
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decodeProgressRecord(raw);
}

/// Read a consumer's v2 progress record, if one has ever been written for
/// it. Used by restart/recovery to derive where a consumer's effects and
/// progress actually left off.
pub fn loadProgress(alloc: Allocator, store: anytype, consumer: []const u8) !?ProgressRecord {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    return try loadProgressTxn(&txn, consumer);
}

fn deleteProgressTxn(txn: anytype, consumer: []const u8) !void {
    var key_buf: [256]u8 = undefined;
    const key = try progressKeyV2(&key_buf, consumer);
    var mutable_txn = txn;
    mutable_txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

// --- Generic applied-sequence/projection-checkpoint API, v2-backed --------
//
// These are the ordinary entry points the rest of the tree calls: a plain
// "what sequence is this consumer at" / "bump it" / "replace its whole
// identity" vocabulary, independent of the WS6 multi-consumer batching
// above. Every one of them reads or writes exactly the v2 key through
// `loadProgressTxn`/`contributeProgressTxn`; none of them knows about a
// second storage location, because there isn't one.

pub fn loadAppliedSequence(alloc: Allocator, store: anytype, index_name: []const u8) !u64 {
    const record = try loadProgress(alloc, store, index_name);
    return if (record) |value| value.applied_sequence else 0;
}

pub fn loadProjectionCheckpoint(alloc: Allocator, store: anytype, index_name: []const u8) !ProjectionCheckpoint {
    const record = try loadProgress(alloc, store, index_name);
    return if (record) |value| value.checkpoint() else .{};
}

/// Advance `index_name` to `sequence`, preserving its current identity
/// (status/generation/config_hash/published_count) — the plain "replay made
/// progress" callback. Identity invalidation (and the consequent serving-
/// certificate epoch revocation) follows the same rule as every other write
/// here: a rewind, or a status/generation/config_hash change, revokes.
pub fn saveAppliedSequence(store: anytype, index_name: []const u8, sequence: u64) !void {
    try saveAppliedSequenceUpdate(store, .{ .index_name = index_name, .sequence = sequence });
}

/// Batch-advance several consumers to the maximum of their current and
/// proposed sequence (a stale/out-of-order replay record cannot rewind a
/// newer cursor, and cannot certify identity/counts it has not reached).
pub fn saveAppliedSequences(store: anytype, updates: []const AppliedSequenceUpdate) !void {
    try applyProgressUpdates(std.heap.page_allocator, store, updates, .advance);
}

/// Exact single-consumer update, carrying whatever identity fields `update`
/// specifies (zero means "unspecified, keep the previous value").
pub fn saveAppliedSequenceUpdate(store: anytype, update: AppliedSequenceUpdate) !void {
    try applyProgressUpdates(std.heap.page_allocator, store, &.{update}, .set);
}

/// Explicit identity replacement: a recovery/rebuild boundary. Every field
/// in `checkpoint` is authoritative, including a deliberate reset to zero;
/// this also clears the index's partial-replay resume point, since a
/// recreated index replaying the same record must not skip items that only
/// the old incarnation applied.
pub fn saveProjectionCheckpoint(store: anytype, index_name: []const u8, checkpoint: ProjectionCheckpoint) !void {
    try applyProgressUpdates(std.heap.page_allocator, store, &.{.{
        .index_name = index_name,
        .sequence = checkpoint.applied_sequence,
        .status = checkpoint.status,
        .generation = checkpoint.generation,
        .config_hash = checkpoint.config_hash,
        .published_count = checkpoint.published_count,
    }}, .replace);
}

pub fn clearAppliedSequence(store: anytype, index_name: []const u8) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try clearAppliedSequenceTxn(&txn, index_name);
    try txn.commit();
}

pub fn clearAppliedSequenceTxn(txn: anytype, index_name: []const u8) !void {
    try artifact_projection_epoch.revoke(txn);
    try deleteProgressTxn(txn, index_name);
    try clearPartialReplayCursorTxn(txn, index_name);
}

/// Live physical resets must revoke completion before touching their files,
/// even when they do not otherwise replace an applied-sequence record.
pub fn invalidateProjectionCompletion(alloc: Allocator, store: anytype) !void {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try artifact_projection_epoch.revoke(&txn);
    try txn.commit();
}

const MetadataUpdateMode = enum { replace, set, advance };

fn applyProgressUpdates(alloc: Allocator, store: anytype, updates: []const AppliedSequenceUpdate, mode: MetadataUpdateMode) !void {
    if (updates.len == 0) return;
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    for (updates) |update| {
        try applyProgressUpdateTxn(&txn, update, mode);
    }
    try txn.commit();
}

fn applyProgressUpdateTxn(txn: anytype, update: AppliedSequenceUpdate, mode: MetadataUpdateMode) !void {
    const previous = if (mode == .replace) null else try loadProgressTxn(txn, update.index_name);
    const previous_checkpoint: ?ProjectionCheckpoint = if (previous) |value| value.checkpoint() else null;
    var next = previous orelse ProgressRecord{};
    var checkpoint = previous_checkpoint orelse ProjectionCheckpoint{};
    if (mode == .replace) {
        checkpoint = update.checkpoint();
    } else {
        const previous_sequence = checkpoint.applied_sequence;
        checkpoint.applied_sequence = if (mode == .advance) @max(previous_sequence, update.sequence) else update.sequence;
        mergeSequenceMetadata(&checkpoint, update, previous_sequence, mode == .set);
    }
    next = next.withCheckpoint(checkpoint);
    if (mode == .replace or invalidatesCompletion(previous_checkpoint, checkpoint)) try artifact_projection_epoch.revoke(txn);
    if (mode == .replace) try clearPartialReplayCursorTxn(txn, update.index_name);
    try contributeProgressTxn(txn, .{
        .consumer = update.index_name,
        .sequence = next.applied_sequence,
        .status = next.status,
        .generation = next.generation,
        .config_hash = next.config_hash,
        .published_count = next.published_count,
        .lsm_wal_checkpointed_through = next.lsm_wal_checkpointed_through,
    });
}

fn mergeSequenceMetadata(
    checkpoint: *ProjectionCheckpoint,
    update: AppliedSequenceUpdate,
    previous_sequence: u64,
    exact_sequence: bool,
) void {
    // A max update cannot certify identity or counts for a replay prefix it
    // has not reached. Exact replacements may intentionally rewind the cut.
    if (!exact_sequence and update.sequence < previous_sequence) return;
    const previous_generation = checkpoint.generation;
    const previous_config_hash = checkpoint.config_hash;
    if (update.generation != 0) checkpoint.generation = update.generation;
    if (update.config_hash != 0) checkpoint.config_hash = update.config_hash;
    const identity_changed = checkpoint.generation != previous_generation or
        checkpoint.config_hash != previous_config_hash;
    const boundary_changed = checkpoint.applied_sequence != previous_sequence;

    // A physical publication certificate is meaningful only for the exact
    // sequence and incarnation that produced it. Advancing/replacing either
    // without a new certificate must invalidate the old one; a stale/lower
    // max update must never rebind its certificate to a newer cursor.
    if (exact_sequence or boundary_changed or identity_changed) {
        checkpoint.published_count = if (update.sequence == checkpoint.applied_sequence)
            update.published_count
        else
            null;
    } else if (update.published_count) |count| {
        if (update.sequence == checkpoint.applied_sequence) checkpoint.published_count = count;
    }
}

fn invalidatesCompletion(previous: ?ProjectionCheckpoint, next: ProjectionCheckpoint) bool {
    const old = previous orelse return true;
    return old.status != next.status or old.generation != next.generation or
        old.config_hash != next.config_hash or next.applied_sequence < old.applied_sequence or
        (next.applied_sequence == old.applied_sequence and old.published_count != null and !std.meta.eql(old.published_count, next.published_count));
}

// --- Coherent multi-consumer read for serving-certificate validation ------

/// One coherently-read view of several consumers' progress, backed by one
/// open read transaction (itself a consistent MVCC snapshot — no file lock
/// needed, unlike the old file-sidecar design this replaces). Acquire
/// before opening a primary write transaction; `requireCurrent` re-checks
/// the authority/epoch fence in that later transaction, since a writer may
/// revoke completion after this snapshot was taken.
pub const ProjectionSnapshot = struct {
    runtime: RuntimeStoreHandle,
    txn: backend_erased.ProbeTxn,
    authority: artifact_publication.Authority,
    epoch: u64,

    pub fn deinit(self: *@This()) void {
        self.txn.abort();
        self.runtime.deinit();
        self.* = undefined;
    }

    /// Absence is not a clean zero-watermark certificate.
    pub fn get(self: *@This(), index_name: []const u8) ?ProjectionCheckpoint {
        const record = (loadProgressTxn(&self.txn, index_name) catch return null) orelse return null;
        return record.checkpoint();
    }

    /// Recheck in the evidence publication transaction. Raw physical resets,
    /// authority switches and reopen can revoke independently of this read.
    pub fn requireCurrent(self: *const @This(), txn: anytype) !void {
        const current = (try artifact_publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(self.authority, current)) return error.ArtifactCatalogDrift;
        try artifact_projection_epoch.requireCurrent(txn, self.epoch);
    }
};

/// Open a coherent read for a batch of index evidence publications, not once
/// per document/requirement. Returns null only when there is no active
/// authority to validate against (nothing to serve from yet).
pub fn tryAcquireProjectionSnapshot(alloc: Allocator, store: anytype) !?ProjectionSnapshot {
    var runtime = try initRuntimeStore(alloc, store);
    errdefer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    errdefer txn.abort();
    const authority = (try artifact_publication.authority(&txn)) orelse {
        txn.abort();
        runtime.deinit();
        return null;
    };
    const epoch = try artifact_projection_epoch.load(&txn);
    return .{ .runtime = runtime, .txn = txn, .authority = authority, .epoch = epoch };
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    pub fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
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

test "contributeProgress writes exactly one commit for a batch of consumers" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    resetProgressCommitCountForTest();
    try contributeProgress(std.testing.allocator, runtime, &.{
        .{ .consumer = "text", .sequence = 5, .generation = 2, .config_hash = 9, .lsm_wal_checkpointed_through = 5 },
        .{ .consumer = "dense_idx", .sequence = 7, .status = .rebuilding },
    }, &.{});
    try std.testing.expectEqual(@as(u64, 1), progressCommitCount());

    const text = (try loadProgress(std.testing.allocator, runtime, "text")).?;
    try std.testing.expectEqual(@as(u64, 5), text.applied_sequence);
    try std.testing.expectEqual(@as(u64, 2), text.generation);
    try std.testing.expectEqual(@as(u64, 9), text.config_hash);
    try std.testing.expectEqual(@as(u64, 5), text.lsm_wal_checkpointed_through);
    try std.testing.expectEqual(ProjectionStatus.clean, text.status);

    const dense = (try loadProgress(std.testing.allocator, runtime, "dense_idx")).?;
    try std.testing.expectEqual(@as(u64, 7), dense.applied_sequence);
    try std.testing.expectEqual(ProjectionStatus.rebuilding, dense.status);
    try std.testing.expectEqual(@as(u64, 0), dense.lsm_wal_checkpointed_through);

    try std.testing.expect((try loadProgress(std.testing.allocator, runtime, "missing")) == null);

    // A second publication is a second commit, not a third write merged with
    // the first: the counter is per-call, not per-consumer.
    try contributeProgress(std.testing.allocator, runtime, &.{
        .{ .consumer = "text", .sequence = 6 },
    }, &.{});
    try std.testing.expectEqual(@as(u64, 2), progressCommitCount());
}

test "contributeProgress piggybacks status writes into the same commit" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    resetProgressCommitCountForTest();
    try contributeProgress(std.testing.allocator, runtime, &.{
        .{ .consumer = "text", .sequence = 1 },
    }, &.{
        .{ .key = "\x00\x00__metadata__:index_status:text", .value = "fake-status-bytes-000001" },
    });
    try std.testing.expectEqual(@as(u64, 1), progressCommitCount());

    var txn = try runtime.beginProbe();
    defer txn.abort();
    try std.testing.expectEqualStrings("fake-status-bytes-000001", try txn.get("\x00\x00__metadata__:index_status:text"));
}

test "progress v2 record rejects corruption" {
    var encoded = encodeProgressRecord(.{ .applied_sequence = 9 });
    encoded[progress_magic_v2.len] ^= 1;
    try std.testing.expectError(error.InvalidDerivedApplyState, decodeProgressRecord(&encoded));
}

test "contributeProgress never leaves progress ahead of what actually committed" {
    // Proxy for "crash between effects and progress": fail the progress
    // commit itself partway through a multi-consumer batch, after a real
    // effects boundary is already durable, and confirm recovery (a fresh
    // read) still reports only the last value that actually committed —
    // never a partially-applied one from the failed attempt. This is what
    // the callers in db.zig rely on: `contributeProgress`/`applyProgressUpdates`
    // are always called strictly after the physical effects, so a crash here
    // can only leave progress behind those effects, never ahead of them, and
    // a failed batch can never advance only some of its consumers.
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    // The durable boundary "effects" reached: sequence 5.
    try contributeProgress(std.testing.allocator, runtime, &.{.{ .consumer = "text", .sequence = 5 }}, &.{});
    try std.testing.expectEqual(@as(u64, 5), (try loadProgress(std.testing.allocator, runtime, "text")).?.applied_sequence);

    // A later attempt batches "text" advancing to 10 together with a second
    // consumer whose name cannot fit the fixed key buffer, failing the
    // whole transaction deterministically (one txn, one commit: a failure
    // encoding any member's key must not leave an earlier member's write
    // behind).
    var long_name_buf: [300]u8 = @splat('x');
    try std.testing.expectError(error.NoSpaceLeft, contributeProgress(std.testing.allocator, runtime, &.{
        .{ .consumer = "text", .sequence = 10 },
        .{ .consumer = &long_name_buf, .sequence = 1 },
    }, &.{}));
    // Never ahead of the last real commit: the failed batch's "text" update
    // never landed even though, taken alone, it would have succeeded.
    try std.testing.expectEqual(@as(u64, 5), (try loadProgress(std.testing.allocator, runtime, "text")).?.applied_sequence);

    // Once allowed to run to completion, the real advance is visible.
    try contributeProgress(std.testing.allocator, runtime, &.{.{ .consumer = "text", .sequence = 10 }}, &.{});
    try std.testing.expectEqual(@as(u64, 10), (try loadProgress(std.testing.allocator, runtime, "text")).?.applied_sequence);
}

test "derived apply state works with memory backend store" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    try saveAppliedSequence(runtime, "idx", 27);
    try std.testing.expectEqual(@as(u64, 27), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "derived apply state works with lsm backend store" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    try saveAppliedSequence(runtime, "idx", 31);
    try std.testing.expectEqual(@as(u64, 31), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "partial replay cursor round-trips per index and is independent of applied progress" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(?PartialReplayCursor, null), try loadPartialReplayCursor(std.testing.allocator, runtime, "text"));
    try savePartialReplayCursor(std.testing.allocator, runtime, "text", .{ .sequence = 9, .offset = 128, .config_hash = 77 });
    try savePartialReplayCursor(std.testing.allocator, runtime, "other", .{ .sequence = 3, .offset = 1, .config_hash = 5 });
    try std.testing.expectEqual(PartialReplayCursor{ .sequence = 9, .offset = 128, .config_hash = 77 }, (try loadPartialReplayCursor(std.testing.allocator, runtime, "text")).?);
    // Overwritten in place as the record advances.
    try savePartialReplayCursor(std.testing.allocator, runtime, "text", .{ .sequence = 9, .offset = 256, .config_hash = 77 });
    try std.testing.expectEqual(@as(u64, 256), (try loadPartialReplayCursor(std.testing.allocator, runtime, "text")).?.offset);
    // Applied progress is a separate record.
    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "text"));
    try std.testing.expectEqual(@as(u64, 3), (try loadPartialReplayCursor(std.testing.allocator, runtime, "other")).?.sequence);
    // Resetting an index's applied progress (drop, recreate, rebuild) clears
    // its resume point, and only its own.
    try saveAppliedSequence(runtime, "text", 8);
    try clearAppliedSequence(runtime, "text");
    try std.testing.expectEqual(@as(?PartialReplayCursor, null), try loadPartialReplayCursor(std.testing.allocator, runtime, "text"));
    try std.testing.expect(try loadPartialReplayCursor(std.testing.allocator, runtime, "other") != null);
    try clearPartialReplayCursor(std.testing.allocator, runtime, "other");
    try std.testing.expectEqual(@as(?PartialReplayCursor, null), try loadPartialReplayCursor(std.testing.allocator, runtime, "other"));
}

test "derived apply state keeps latest lsm value across many overwrites, including after reopen" {
    // Preserve leak checks; allocation backtraces are opt-in for diagnostics.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    var tmp = @import("../../../common/test_directory.zig").fastTmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPath(std.testing.io, &path_buf);
    const path = path_buf[0..path_len];

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 4,
            .level_target_runs_base = 2,
            .level_target_runs_multiplier = 2,
            .level_target_bytes_base = 0,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        try saveAppliedSequence(runtime, "idx", 0);
        var sequence: u64 = 1;
        while (sequence <= 1024) : (sequence += 1) {
            try saveAppliedSequence(runtime, "idx", sequence);
        }
        try std.testing.expectEqual(@as(u64, 1024), try loadAppliedSequence(alloc, runtime, "idx"));
    }

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 4,
            .level_target_runs_base = 2,
            .level_target_runs_multiplier = 2,
            .level_target_bytes_base = 0,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        try std.testing.expectEqual(@as(u64, 1024), try loadAppliedSequence(alloc, runtime, "idx"));
    }
}

test "derived apply state clear removes persisted sequence" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "idx", 41);
    try clearAppliedSequence(runtime, "idx");
    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "projection checkpoint persists status and identity fields, and an explicit replacement resets them" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveProjectionCheckpoint(runtime, "dense_idx", .{
        .applied_sequence = 44,
        .status = .degraded,
        .generation = 9,
        .config_hash = 0x1234,
        .published_count = 37,
    });

    const checkpoint = try loadProjectionCheckpoint(std.testing.allocator, runtime, "dense_idx");
    try std.testing.expectEqual(@as(u64, 44), checkpoint.applied_sequence);
    try std.testing.expectEqual(ProjectionStatus.degraded, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 9), checkpoint.generation);
    try std.testing.expectEqual(@as(u64, 0x1234), checkpoint.config_hash);
    try std.testing.expectEqual(@as(?u64, 37), checkpoint.published_count);
    try std.testing.expectEqual(@as(u64, 44), try loadAppliedSequence(std.testing.allocator, runtime, "dense_idx"));

    // Ordinary replay preserves lifecycle and expires a count certificate
    // when its physical boundary advances. A stale max update cannot move
    // it back.
    try saveAppliedSequences(runtime, &.{.{ .index_name = "dense_idx", .sequence = 45 }});
    try saveAppliedSequences(runtime, &.{.{ .index_name = "dense_idx", .sequence = 44, .published_count = 99 }});
    var advanced = checkpoint;
    advanced.applied_sequence = 45;
    advanced.published_count = null;
    try std.testing.expectEqualDeep(advanced, try loadProjectionCheckpoint(std.testing.allocator, runtime, "dense_idx"));
    try saveAppliedSequence(runtime, "dense_idx", 46);
    advanced.applied_sequence = 46;
    try std.testing.expectEqualDeep(advanced, try loadProjectionCheckpoint(std.testing.allocator, runtime, "dense_idx"));

    // Explicit replacement admits a clean new incarnation, even at zero.
    const replacement = ProjectionCheckpoint{ .generation = 4, .config_hash = 43, .published_count = 0 };
    try saveProjectionCheckpoint(runtime, "dense_idx", replacement);
    try std.testing.expectEqualDeep(replacement, try loadProjectionCheckpoint(std.testing.allocator, runtime, "dense_idx"));

    try clearAppliedSequence(runtime, "dense_idx");
    try std.testing.expectEqualDeep(ProjectionCheckpoint{}, try loadProjectionCheckpoint(std.testing.allocator, runtime, "dense_idx"));
}

test "max replay updates preserve newer projection identity and set updates are exact" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    const certified: ProjectionCheckpoint = .{ .applied_sequence = 101, .generation = 2, .config_hash = 42, .published_count = 3 };
    try saveProjectionCheckpoint(runtime, "text", certified);

    // A stale max update cannot certify identity or counts for a prefix it
    // has not reached.
    try saveAppliedSequences(runtime, &.{.{ .index_name = "text", .sequence = 99, .generation = 7, .config_hash = 77, .published_count = 9 }});
    try std.testing.expectEqualDeep(certified, try loadProjectionCheckpoint(std.testing.allocator, runtime, "text"));

    // Once an update reaches a new cut, its own identity/count proof is
    // published, and no older certificate is rebound to that newer cursor.
    try saveAppliedSequences(runtime, &.{.{ .index_name = "text", .sequence = 102, .generation = 3, .config_hash = 43, .published_count = 4 }});
    try std.testing.expectEqualDeep(ProjectionCheckpoint{ .applied_sequence = 102, .generation = 3, .config_hash = 43, .published_count = 4 }, try loadProjectionCheckpoint(std.testing.allocator, runtime, "text"));

    // Explicit sequence replacement (set mode) remains authoritative at an
    // older cut.
    try saveAppliedSequenceUpdate(runtime, .{ .index_name = "text", .sequence = 99, .generation = 4, .config_hash = 44, .published_count = 2 });
    try std.testing.expectEqualDeep(ProjectionCheckpoint{ .applied_sequence = 99, .generation = 4, .config_hash = 44, .published_count = 2 }, try loadProjectionCheckpoint(std.testing.allocator, runtime, "text"));
}

test "projection checkpoint replacement revokes serving-certificate completion" {
    const publication = @import("../artifact_publication.zig");
    const epoch = @import("../artifact_projection_epoch.zig");
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();
    {
        var txn = try runtime.beginWrite();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.commit();
    }
    const Probe = struct {
        fn current(store: *docstore_mod.DocStore) !u64 {
            var read = try store.beginReadTxn();
            defer read.abort();
            return epoch.load(&read);
        }
    };
    try saveProjectionCheckpoint(runtime, "text", .{ .applied_sequence = 10, .generation = 1, .config_hash = 7 });
    const first = try Probe.current(runtime);
    try std.testing.expectEqual(@as(u64, 1), first);
    try saveAppliedSequences(runtime, &.{.{ .index_name = "text", .sequence = 11, .config_hash = 7 }});
    try std.testing.expectEqual(first, try Probe.current(runtime));
    try saveAppliedSequenceUpdate(runtime, .{ .index_name = "text", .sequence = 12, .config_hash = 7 });
    try std.testing.expectEqual(first, try Probe.current(runtime));
    // Rebuilding, even at the same source cut, invalidates prior completion.
    try saveProjectionCheckpoint(runtime, "text", .{ .applied_sequence = 12, .status = .rebuilding, .generation = 2, .config_hash = 7 });
    try std.testing.expectEqual(first + 1, try Probe.current(runtime));
    // Recovery may deliberately lower a watermark; it cannot retain a prefix
    // captured from the old physical projection.
    try saveAppliedSequence(runtime, "text", 3);
    try std.testing.expectEqual(first + 2, try Probe.current(runtime));
    try clearAppliedSequence(runtime, "text");
    try std.testing.expectEqual(first + 3, try Probe.current(runtime));
    try saveAppliedSequence(runtime, "text", 20);
    try std.testing.expectEqual(first + 4, try Probe.current(runtime));
    try saveAppliedSequence(runtime, "text", 21);
    try std.testing.expectEqual(first + 4, try Probe.current(runtime));
    {
        var txn = try runtime.beginWrite();
        defer txn.abort();
        try clearAppliedSequenceTxn(&txn, "text");
        try std.testing.expectEqual(first + 5, try epoch.load(&txn));
    }
    try std.testing.expectEqual(first + 4, try Probe.current(runtime));
    try saveAppliedSequence(runtime, "text", 2);
    try std.testing.expectEqual(first + 5, try Probe.current(runtime));
}

test "ordered artifact inventory projection snapshot owns a coherent checkpoint and publication fence" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const publication = @import("../artifact_publication.zig");
    const epoch = @import("../artifact_projection_epoch.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/projection-snapshot", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try std.testing.expect(try tryAcquireProjectionSnapshot(alloc, db.core.store) == null);
    var activation: publication.Command = .{ .mode = .activate, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, activation);
        try txn.commit();
    }
    try saveProjectionCheckpoint(db.core.store, "text", .{ .applied_sequence = 11, .generation = 2, .config_hash = 7 });
    var snapshot = (try tryAcquireProjectionSnapshot(alloc, db.core.store)).?;
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(u64, 11), snapshot.get("text").?.applied_sequence);
    try std.testing.expect(snapshot.get("missing") == null);
    {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        try snapshot.requireCurrent(&txn);
    }
    // Physical reset/open revocation needs no file lock to invalidate a
    // snapshot already taken: the next primary transaction simply disagrees.
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try epoch.revoke(&txn);
        try std.testing.expectError(error.EnrichmentSourceChanged, snapshot.requireCurrent(&txn));
        try txn.commit();
    }
    {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, snapshot.requireCurrent(&txn));
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        activation.authority_epoch += 1;
        try publication.stageAuthority(&txn, activation);
        try std.testing.expectError(error.ArtifactCatalogDrift, snapshot.requireCurrent(&txn));
        try txn.commit();
    }
}
