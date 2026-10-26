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

//! Document key-value store with centralized binary key encoding.
//!
//! Public document IDs are raw byte strings. Internal primary, TTL, artifact,
//! chunk, and graph records are encoded through storage/internal_keys.zig so
//! user-controlled IDs never share a delimiter namespace with derived records.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const backend_types = @import("backend_types.zig");
const change_journal_mod = @import("db/derived/change_journal.zig");
const internal_keys = @import("internal_keys.zig");
const retained_effects = @import("retained_effects.zig");
const artifact_payload = @import("artifact_payload.zig");
const document_payload = @import("document_payload.zig");
const lsm_backend = @import("lsm_backend.zig");
const mem_backend = @import("mem_backend.zig");
const platform_time = @import("antfly_platform").time;
const writer_locked_retry_count: usize = 1000;
const writer_locked_retry_sleep_ns: u64 = 100_000;

fn backoffWriterLockRetry(io: ?std.Io) void {
    if (io) |active_io| {
        active_io.sleep(std.Io.Duration.fromNanoseconds(@intCast(writer_locked_retry_sleep_ns)), .awake) catch {};
        return;
    }
    if (comptime builtin.os.tag == .freestanding) return;
    std.Io.Threaded.global_single_threaded.io().sleep(.fromNanoseconds(@intCast(writer_locked_retry_sleep_ns)), .awake) catch {};
}

const replay_hints = [_]change_journal_mod.TargetHint{
    .enrichment,
    .full_text,
    .dense_vector,
    .sparse_vector,
    .graph,
    .algebraic,
    .resolution,
    .promotion,
};

fn replayHintOrdinal(hint: change_journal_mod.TargetHint) u8 {
    return @intCast(@backingInt(hint));
}

fn replayHintFromSingleMask(mask: u8) ?change_journal_mod.TargetHint {
    if (mask == 0 or (mask & (mask - 1)) != 0) return null;
    inline for (@typeInfo(change_journal_mod.TargetHint).@"enum".field_values) |field_value| {
        if (mask == (@as(u8, 1) << @intCast(field_value))) return @fromBackingInt(@intCast(field_value));
    }
    return null;
}

fn encodeReplayNextSequence(sequence: u64) [8]u8 {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, sequence, .little);
    return raw;
}

fn encodeReplaySequence(sequence: u64) [8]u8 {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, sequence, .little);
    return raw;
}

fn decodeReplaySequence(raw: []const u8) ?u64 {
    if (raw.len != 8) return null;
    return std.mem.readInt(u64, raw[0..8], .little);
}

fn isEmbeddingReplayArtifactKey(key: []const u8) bool {
    return internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key);
}

fn appendReplayArtifactsForHint(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    artifact_keys: []const []const u8,
    hint: change_journal_mod.TargetHint,
) !void {
    for (artifact_keys) |key| {
        const keep = switch (hint) {
            .dense_vector, .sparse_vector => isEmbeddingReplayArtifactKey(key),
            .graph => internal_keys.isGraphEdgeArtifactKey(key) or
                internal_keys.isAssetArtifactKey(key) or
                internal_keys.isChunkArtifactRecordKey(key) or
                internal_keys.isResolutionArtifactKey(key),
            // Resolution artifact keys reach the resolution stage too: a
            // committed sibling resolution re-drives event-identity
            // composition over the shared source artifact.
            .resolution => internal_keys.isAssetArtifactKey(key) or
                internal_keys.isResolutionArtifactKey(key),
            .promotion => internal_keys.isResolutionArtifactKey(key),
            .enrichment, .full_text, .algebraic => false,
        };
        if (keep) try out.append(alloc, key);
    }
}

fn encodeReplayPayloadForHint(
    alloc: Allocator,
    record: change_journal_mod.Record,
    hint: change_journal_mod.TargetHint,
) ![]u8 {
    var target_hints = [_]change_journal_mod.TargetHint{hint};
    var artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer artifact_keys.deinit(alloc);
    try appendReplayArtifactsForHint(alloc, &artifact_keys, record.changed_artifact_keys, hint);

    var filtered = change_journal_mod.Record{
        .version = record.version,
        .sequence = record.sequence,
        .target_hints = target_hints[0..],
    };
    switch (hint) {
        .enrichment => {
            filtered.changed_doc_keys = record.changed_doc_keys;
        },
        .full_text => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
        },
        .algebraic => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
        },
        .dense_vector, .sparse_vector => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .graph => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .resolution => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .promotion => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
    }
    return try change_journal_mod.encodeRecord(alloc, filtered);
}

fn writeOriginalReplayHintEntries(txn: anytype, sequence: u64, mask: u8, payload: []const u8) !void {
    const latest_raw = encodeReplaySequence(sequence);
    for (replay_hints) |hint| {
        if ((mask & change_journal_mod.singleHintMask(hint)) == 0) continue;
        const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), sequence);
        try txn.put(key[0..], payload);
        const latest_key = internal_keys.replayLatestSequenceKey(replayHintOrdinal(hint));
        try txn.put(latest_key[0..], latest_raw[0..]);
    }
}

fn writeReplayEntries(alloc: Allocator, txn: anytype, sequence: u64, payload: []const u8) !void {
    // DB writers reserve and commit under the apply lock. Failed reservations
    // may leave holes, but a late writer must never fill a hole below an
    // already published cut: replay consumers may have passed it. Consensus
    // duplicate detection happens before opening this mutation transaction.
    if (sequence == std.math.maxInt(u64)) return error.InvalidBatchRequest;
    const previous = txn.get(internal_keys.replay_meta_next_sequence_key[0..]) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (previous) |raw| {
        if (raw.len != 8) return error.CorruptReplayMetadata;
        if (sequence < std.mem.readInt(u64, raw[0..8], .little)) return error.InvalidBatchRequest;
    }
    try txn.put(internal_keys.replay_meta_init_key[0..], "");
    const next_raw = encodeReplayNextSequence(sequence + 1);
    try txn.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
    const latest_raw = encodeReplaySequence(sequence);

    const all_key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, sequence);
    try txn.put(all_key[0..], payload);
    const all_latest_key = internal_keys.replayLatestSequenceKey(internal_keys.replay_all_kind);
    try txn.put(all_latest_key[0..], latest_raw[0..]);

    const mask = change_journal_mod.encodedRecordHintMask(payload) catch return;
    if (mask == 0) return;

    var decoded = change_journal_mod.decodeRecord(alloc, payload) catch {
        try writeOriginalReplayHintEntries(txn, sequence, mask, payload);
        return;
    };
    defer decoded.deinit();

    for (replay_hints) |hint| {
        if ((mask & change_journal_mod.singleHintMask(hint)) == 0) continue;
        const lane_payload = try encodeReplayPayloadForHint(alloc, decoded.record, hint);
        defer alloc.free(lane_payload);
        const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), sequence);
        try txn.put(key[0..], lane_payload);
        const latest_key = internal_keys.replayLatestSequenceKey(replayHintOrdinal(hint));
        try txn.put(latest_key[0..], latest_raw[0..]);
    }
}
// ============================================================================
// KV types
// ============================================================================

pub const KVPair = struct {
    key: []const u8,
    value: []const u8,
};

pub const OwnedKVPair = struct {
    key: []u8,
    value: []u8,
};

fn appendOwnedKVPairCopy(
    alloc: Allocator,
    results: *std.ArrayListUnmanaged(OwnedKVPair),
    key: []const u8,
    value: []const u8,
) !void {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try results.append(alloc, .{ .key = owned_key, .value = owned_value });
}

pub const ReplayIterationStats = struct {
    scanned_entries: usize = 0,
    matched_entries: usize = 0,
    last_sequence: u64 = 0,
    hint_filter_skips: usize = 0,
    scan_batches: usize = 0,
    fallback_used: bool = false,
};

// ============================================================================
// ByteRange — shard ownership range
// ============================================================================

pub const ByteRange = @import("byte_range.zig").ByteRange;

// ============================================================================
// DocStore
// ============================================================================

pub const DocStoreOptions = struct {
    map_size: usize = 256 * 1024 * 1024,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    read_only: bool = false,
};

fn applyGraphRetirement(txn: anytype, alloc: Allocator, key: []const u8, value: []const u8, maybe: *?bool) anyerror!void {
    const generation = try @import("graph_cleanup_contract.zig").retirementGeneration(value);
    if (generation != 0) {
        const previous = txn.get(internal_keys.graph_endpoint_cleanup_generation_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous != null and previous.?.len != 8) return error.InvalidGraphRetirement;
        const prior = if (previous) |bytes| std.mem.readInt(u64, bytes[0..8], .little) else 0;
        if (generation > prior) {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, generation, .little);
            try txn.put(internal_keys.graph_endpoint_cleanup_generation_key, &bytes);
        }
    }
    const artifact = try internal_keys.graphRetirementArtifactKeyAlloc(alloc, key);
    defer alloc.free(artifact);
    if (try retirementSuppressesRelationship(txn, alloc, artifact, value)) {
        txn.delete(artifact) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    const ref = try internal_keys.graphRetirementRefKeyAlloc(alloc, key);
    defer alloc.free(ref);
    if (txn.get(ref)) |_| {} else |err| {
        if (err != error.NotFound) return err;
        const count = try graphRetirementCount(txn);
        const next = try std.math.add(u64, count, 1);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, next, .little);
        try txn.put(ref, "1");
        try txn.put(internal_keys.graph_retirement_count_key, &bytes);
    }
    // Old stores remain conservative until the bounded v3 migration completes.
    try txn.put(internal_keys.graph_retirement_present_key, "1");
    maybe.* = true;
}

fn graphRetirementCount(txn: anytype) !u64 {
    const bytes = txn.get(internal_keys.graph_retirement_count_key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    if (bytes.len != 8) return error.InvalidGraphRetirement;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

fn removeGraphRetirementReference(txn: anytype, alloc: Allocator, key: []const u8, maybe: *?bool) anyerror!void {
    if (!internal_keys.isGraphRetirementKey(key)) return;
    const ref = try internal_keys.graphRetirementRefKeyAlloc(alloc, key);
    defer alloc.free(ref);
    _ = txn.get(ref) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const next = try std.math.sub(u64, try graphRetirementCount(txn), 1);
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, next, .little);
    try txn.delete(ref);
    try txn.put(internal_keys.graph_retirement_count_key, &bytes);
    maybe.* = null;
}

fn graphRetirementsPresentCached(txn: anytype, maybe: *?bool) !bool {
    if (maybe.* == null) {
        if (txn.get(internal_keys.graph_incoming_ready_key)) |_| {
            maybe.* = try graphRetirementCount(txn) != 0;
            return maybe.*.?;
        } else |err| if (err != error.NotFound) return err;
        // Without a complete local directory, absence of metadata proves
        // nothing about primary markers (physical range copies omit metadata).
        maybe.* = true;
    }
    return maybe.*.?;
}

fn graphRelationshipRetiredCached(txn: anytype, alloc: Allocator, key: []const u8, maybe: *?bool) !bool {
    if (!internal_keys.isGraphEdgeArtifactKey(key)) return false;
    if (!try graphRetirementsPresentCached(txn, maybe)) return false;
    return graphRelationshipRetiredInTxn(txn, alloc, key);
}

fn graphRelationshipRetiredInTxn(txn: anytype, alloc: Allocator, key: []const u8) !bool {
    if (!internal_keys.isGraphEdgeArtifactKey(key)) return false;
    const retirement = try internal_keys.graphRetirementKeyAlloc(alloc, key);
    defer alloc.free(retirement);
    const stamp = txn.get(retirement) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return retirementSuppressesRelationship(txn, alloc, key, stamp);
}

fn retirementSuppressesRelationship(txn: anytype, alloc: Allocator, key: []const u8, stamp: []const u8) !bool {
    const contract = @import("graph_cleanup_contract.zig");
    const generation = try contract.retirementGeneration(stamp);
    const end = internal_keys.findComponentTerminator(key, 1) orelse return error.InvalidGraphSegment;
    const owner = try internal_keys.decodeBodyAlloc(alloc, key[1..end]);
    defer alloc.free(owner);
    const job_key = try contract.ownerJobKeyAlloc(alloc, owner);
    defer alloc.free(job_key);
    const job_value = txn.get(job_key) catch |err| switch (err) {
        error.NotFound => return true,
        else => return err,
    };
    // A changed owner starts a new lifecycle atomically. Its fresh document
    // projection may publish before bounded history collection has finished.
    return generation >= (try contract.decodeOwnerJob(job_key, job_value)).generation;
}

fn columnarMutationToken(txn: anytype, cached: *?internal_keys.ColumnarMutationToken) !internal_keys.ColumnarMutationToken {
    if (cached.*) |token| return token;
    const old = txn.get(internal_keys.relational_columnar_mutation_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const previous = if (old) |bytes| blk: {
        if (bytes.len != 8) return error.InvalidColumnMutationVersion;
        break :blk std.mem.readInt(u64, bytes[0..8], .little);
    } else 0;
    const version = std.math.add(u64, previous, 1) catch return error.ColumnMutationVersionExhausted;
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, version, .little);
    try txn.put(internal_keys.relational_columnar_mutation_key, &bytes);
    const token = internal_keys.relationalColumnarMutationToken(version);
    cached.* = token;
    return token;
}

fn updateGraphEndpointCleanupAdmission(txn: anytype, alloc: Allocator, key: []const u8, endpoint: ?[]const u8) anyerror!void {
    if (!std.mem.startsWith(u8, key, internal_keys.graph_endpoint_cleanup_prefix)) return;
    const decoded_endpoint = if (endpoint) |value| (try @import("graph_cleanup_contract.zig").decode(key, value)).endpoint else null;
    const ref = try std.mem.concat(alloc, u8, &.{ internal_keys.graph_endpoint_cleanup_ref_prefix, key[internal_keys.graph_endpoint_cleanup_prefix.len..] });
    defer alloc.free(ref);
    const raw_count = txn.get(internal_keys.graph_endpoint_cleanup_count_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var count: u64 = 0;
    if (raw_count) |raw| {
        if (raw.len != 8) return error.InvalidGraphSegment;
        count = std.mem.readInt(u64, raw[0..8], .little);
    } else {
        // A partial older queue has no complete admission summary. Preserve
        // the conservative fallback until it drains, then initialize anew.
        if (try txn.hasPrefix(internal_keys.graph_endpoint_cleanup_prefix)) {
            if (endpoint == null) txn.delete(ref) catch |err| if (err != error.NotFound) return err;
            return;
        }
    }
    const already_active = if (txn.get(ref)) |_| true else |err| if (err == error.NotFound) false else return err;
    if (decoded_endpoint) |target| {
        if (!already_active) {
            const ready = if (txn.get(internal_keys.graph_incoming_ready_key)) |_| true else |err| if (err == error.NotFound) false else return err;
            var active = !ready;
            if (ready) {
                const prefix = try internal_keys.graphIncomingPrefixAlloc(alloc, target);
                defer alloc.free(prefix);
                active = try txn.hasPrefix(prefix);
            }
            if (active) {
                count = try std.math.add(u64, count, 1);
                try txn.put(ref, "1");
            }
        }
    } else if (already_active) {
        count = try std.math.sub(u64, count, 1);
        try txn.delete(ref);
    }
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, count, .little);
    try txn.put(internal_keys.graph_endpoint_cleanup_count_key, &bytes);
}

pub const DocStore = struct {
    payload_store: ?artifact_payload.Store = null,
    payload_capture_inline: bool = false,
    payload_migration_allowance: ?u64 = null,
    payload_policy_mutex: std.Io.Mutex = .init,
    payload_recovery_required: std.atomic.Value(bool) = .init(false),
    alloc: Allocator,
    /// Process-local wake hint, published only after successful row/schema
    /// commits. Durable mutation IDs and timers remain the restart authority.
    columnar_revision: @import("antfly_platform").atomic.Value(u64) = .init(0),
    /// Owner-local invalidation for retained-query reuse. Includes primary
    /// metadata/graph/enrichment commits which need not append source replay.
    /// Increment before attempting commit: ambiguous backend outcomes invalidate.
    query_generation_revision: @import("antfly_platform").atomic.Value(u64) = .init(0),
    source_certificates: @import("source_proof_registry.zig").Registry(@import("document_mutation_revision.zig").Content) = .{},
    source_bulk_fences: std.atomic.Value(usize) = .init(0),
    // 0 unknown, 1 no retention catalog, 2 catalog may exist. Admission marks
    // this before its commit; an aborted admission merely leaves a safe probe.
    retained_effects_cache: std.atomic.Value(u8) = .init(0),
    // Internal qualification gate for legacy promotion, not a user opt-in.
    // DBCore also sets this before publication when opening an already-admitted
    // native schema, preserving its read/retirement semantics after restart.
    // New promotion still requires reader/peer cohort qualification.
    document_payload_prototype: bool = false,
    /// All readers of this owner were qualified by an ordered cluster barrier,
    /// or by the single-owner execution path. Never set from a public request.
    document_reader_cohort: std.atomic.Value(u32) = .init(0),
    document_native_active: std.atomic.Value(bool) = .init(false),
    document_payload_inline: bool = false,
    document_values_possible: std.atomic.Value(u8) = .init(0),
    document_prepared_stages: std.atomic.Value(u64) = .init(0),
    document_manifest_cache: std.atomic.Value(?*document_payload.ManifestCache) = .init(null),
    runtime_store: backend_erased.Store,
    owns_runtime_store: bool,
    owned_lsm_backend: ?lsm_backend.BackendHandle,
    replay_index_state: std.atomic.Value(u8),
    next_replay_sequence_cached: AtomicU64,
    // Reservations are not visibility. Zero means the durable committed cut
    // has not yet been loaded; intentional persisted replay floors count.
    committed_replay_next_cached: AtomicU64 = .init(0),
    // A failed portable-import rollback leaves a durable recovery marker. Once
    // fenced, this handle must not expose the partial generation or accept
    // writes that restart recovery would later erase. Reopening constructs a
    // fresh handle and completes marker recovery before returning it.
    portable_import_recovery_required: std.atomic.Value(bool) = .init(false),
    // Directory publication and native generation adoption have a durable
    // commit point followed by an in-memory schema/catalog rebind. Normal
    // readers must observe neither an incremental copy nor a new keyspace
    // through the old runtime schema. Bounded atomic admission remains
    // substantially cheaper than taking DB's global apply lock on hot reads.
    // The high bit closes admission and the remaining bits count admitted
    // transactions. Packing both into one atomic makes reader admission versus
    // publication closure a single indivisible transition.
    portable_import_reader_state: std.atomic.Value(usize) = .init(0),

    pub fn qualifyDocumentReaders(self: *DocStore, minimum: u32) !void {
        if (minimum != 29 or minimum > @import("schema.zig").maximum_reader_format_version) return error.UnsupportedVersion;
        self.document_reader_cohort.store(minimum, .release);
    }

    pub fn documentLayoutEnabled(self: *const DocStore) bool {
        return self.document_payload_prototype or self.document_native_active.load(.acquire);
    }

    pub fn documentMigrationEnabled(self: *const DocStore) bool {
        return self.documentLayoutEnabled() or self.document_reader_cohort.load(.acquire) != 0;
    }

    pub fn adoptDocumentReaderFormat(self: *DocStore, minimum: u32) !void {
        try self.qualifyDocumentReaders(minimum);
        self.document_native_active.store(true, .release);
    }

    pub fn documentMinimumReader(self: *const DocStore) u32 {
        return @max(@as(u32, 26), self.document_reader_cohort.load(.acquire));
    }

    pub fn configureDocumentCapture(self: *const DocStore, capture: *@import("document_mutation_revision.zig").Capture) void {
        capture.logical_commitments = self.documentMigrationEnabled();
        capture.logical_minimum_bytes = @import("document_projection.zig").minimum_bytes;
        capture.minimum_reader_format = self.documentMinimumReader();
    }

    const replay_index_unknown: u8 = 0;
    const replay_index_missing: u8 = 1;
    const replay_index_available: u8 = 2;
    const portable_import_publication_bit: usize = @as(usize, 1) << (@bitSizeOf(usize) - 1);
    const portable_import_reader_count_mask: usize = ~portable_import_publication_bit;

    pub const BackendStore = backend_adapter.Store(DocStore, Txn, Txn, Batch, .{
        .capabilities = backendCapabilities,
        .begin_read = beginReadTxn,
        .begin_probe = beginProbeTxn,
        .begin_current_scan = beginCurrentScanTxn,
        .begin_write = beginWriteTxn,
        .begin_batch = beginWriteBatch,
    });

    pub const Txn = struct {
        range_mutation: @import("range_protection.zig").Mutation = .{},
        retained: retained_effects.Capture = .{},
        artifact_inputs: @import("artifact_input_capture.zig").Capture = .{},
        artifact_footprint: @import("artifact_footprint.zig").Capture = .{},
        document_revisions: @import("document_mutation_revision.zig").Capture = .{},
        mutation_capture: ?*@import("txn_mutation_capture.zig").Capture = null,
        payload_session: ?*artifact_payload.Session = null,
        document_arena: ?std.heap.ArenaAllocator = null,
        alloc: Allocator,
        read: ?backend_erased.ReadTxn = null,
        probe: ?backend_erased.ProbeTxn = null,
        current_scan: ?backend_erased.CurrentScanTxn = null,
        write: ?backend_erased.WriteTxn = null,
        portable_import_reader_owner: ?*DocStore = null,
        columns_invalidated: bool = false,
        columnar_mutation: ?internal_keys.ColumnarMutationToken = null,
        graph_directory_checked: bool = false,
        graph_retirements_maybe: ?bool = null,
        columnar_owner: ?*DocStore = null,

        /// Fork an immutable runtime read at the same visibility cut. Each
        /// fork has independent cursor scratch while the erased backend pins
        /// the original snapshot until its last child closes. Callers must
        /// serialize use of the payload session, which is shared for the
        /// lifetime of this read family.
        pub fn forkRead(self: *Txn) !Txn {
            if (self.write != null or self.probe != null or self.current_scan != null)
                return error.ReadSnapshotForkUnsupported;
            const parent = if (self.read) |*read| read else return error.ReadSnapshotForkUnsupported;
            const owner = self.portable_import_reader_owner orelse return error.ReadSnapshotForkUnsupported;
            try owner.acquirePortableImportReader();
            errdefer owner.releasePortableImportReader();
            var fork = try parent.forkRead();
            errdefer fork.abort();
            if (self.payload_session) |session| session.retain();
            return .{
                .alloc = self.alloc,
                .read = fork,
                .payload_session = self.payload_session,
                .portable_import_reader_owner = owner,
            };
        }

        /// Preserve the immutable visibility cut through the erased Store
        /// adapter too. The returned handle retains its payload session and
        /// import-reader admission independently; it may outlive this handle.
        pub fn forkBorrowedRead(self: *Txn) !Txn {
            return self.forkRead();
        }

        pub const CursorAdapter = backend_erased.Cursor;
        pub const ReadAdapter = backend_adapter.ReadTxn(Txn, CursorAdapter, .{
            .abort = Txn.abort,
            .get = Txn.get,
            .open_cursor = Txn.openCursorAdapter,
        });
        pub const WriteAdapter = backend_adapter.WriteTxn(Txn, CursorAdapter, .{
            .abort = Txn.abort,
            .commit = Txn.commit,
            .get = Txn.get,
            .put = Txn.put,
            .delete = Txn.delete,
            .open_cursor = Txn.openCursorAdapter,
        });

        pub fn abort(self: *Txn) void {
            if (self.document_arena) |*arena| arena.deinit();
            self.retained.deinit(self.alloc);
            self.artifact_inputs.deinit(self.alloc);
            self.document_revisions.deinit(self.alloc);
            const reader_owner = self.portable_import_reader_owner;
            const payload_session = self.payload_session;
            defer if (payload_session) |session| session.release();

            if (self.write) |*write| {
                write.abort();
            } else if (self.current_scan) |*current_scan| {
                current_scan.abort();
            } else if (self.probe) |*probe| {
                probe.abort();
            } else if (self.read) |*read| {
                read.abort();
            }
            self.* = undefined;
            if (reader_owner) |owner| owner.releasePortableImportReader();
        }

        /// A compound metadata operation must not be committed after any of
        /// its fallible stages failed, even if its caller catches the error.
        pub fn poison(self: *Txn) void {
            self.retained.poisoned = true;
            self.document_revisions.poisoned = true;
        }

        pub fn commit(self: *Txn) !void {
            if (self.columnar_owner) |owner| _ = owner.query_generation_revision.fetchAdd(1, .release);
            errdefer self.retained.poisoned = true;
            // A backend error need not prove that its commit was absent.
            // Reload the durable cut on the next observation, but never
            // rewind the reservation allocator (the outcome is ambiguous).
            errdefer if (self.columnar_owner) |owner| owner.committed_replay_next_cached.store(0, .release);
            try self.retained.stage(self.alloc, self);
            {
                self.retained.staging = true;
                defer self.retained.staging = false;
                try self.artifact_inputs.stage(self, self.retained.staged);
                try self.artifact_footprint.stage(self);
                try self.document_revisions.stage(self);
            }
            const reader_owner = self.portable_import_reader_owner;
            const columnar_owner = if (self.columnar_mutation != null or self.columns_invalidated) self.columnar_owner else null;
            const source_owner = self.columnar_owner;
            if (source_owner) |owner| owner.source_certificates.begin(&self.document_revisions.documents);
            defer if (source_owner) |owner| owner.source_certificates.end();
            const payload_session = self.payload_session;
            if (payload_session) |session| try session.stageReferenceEpoch(self);
            if (payload_session) |session| try session.prepareCommit();
            if (payload_session) |session| session.primary_commit_attempted = true;
            if (self.write) |*write| {
                try write.commit();
            } else {
                return error.ReadOnly;
            }
            if (payload_session) |session| {
                session.committed = true;
                session.release();
            }
            if (self.document_arena) |*arena| arena.deinit();
            self.retained.deinit(self.alloc);
            self.artifact_inputs.deinit(self.alloc);
            self.document_revisions.deinit(self.alloc);
            self.* = undefined;
            if (columnar_owner) |owner| _ = owner.columnar_revision.fetchAdd(1, .release);
            if (reader_owner) |owner| owner.releasePortableImportReader();
        }

        pub fn documentAllocator(self: *Txn) Allocator {
            if (self.document_arena == null) self.document_arena = .init(@import("antfly_platform").allocator.reclaimingScratch(self.alloc));
            return self.document_arena.?.allocator();
        }
        pub fn get(self: *Txn, key: []const u8) ![]const u8 {
            const value = try self.getPhysical(key);
            if (internal_keys.isStoredDocumentRowKey(key) and document_payload.isManifest(value)) {
                if (self.probe != null) {
                    const owner = self.portable_import_reader_owner orelse return error.Unsupported;
                    var pinned = try owner.beginReadTxn();
                    defer pinned.abort();
                    return try self.documentAllocator().dupe(u8, try pinned.get(key));
                }
                return document_payload.read(self.documentAllocator(), self, key, value);
            }
            if (self.payload_session) |session| return try session.get(key, value);
            // A live probe admitted before migration may observe a later
            // reference. Retry with a new source lease; never leak its encoding
            // or acquire a lease after reading a potentially retired reference.
            if (artifact_payload.isEmbeddingKey(key) and artifact_payload.isReference(value))
                return error.VectorMigrationReadEpochChanged;
            return value;
        }

        /// Exact key presence in this transaction's view. Physical references
        /// prove presence without resolving external source/artifact payloads.
        pub fn contains(self: *Txn, key: []const u8) !bool {
            if (self.write) |*write| return write.contains(key);
            if (self.probe) |*probe| return try probe.contains(key);
            _ = self.getPhysical(key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            return true;
        }

        pub fn getArtifactMetadata(self: *Txn, key: []const u8) !artifact_payload.Metadata {
            const value = try self.getPhysical(key);
            const metadata = try artifact_payload.Metadata.decode(value);
            // Same-binary qualification control for the cost of reconstructing
            // source payloads in metadata consumers. Keep the API result equal.
            const control = if (@import("builtin").link_libc) std.c.getenv("ANTFLY_SOURCE_VECTOR_METADATA_ONLY") else null;
            if (control) |raw| {
                if (std.mem.eql(u8, std.mem.span(raw), "0")) {
                    if (self.payload_session) |session| _ = try session.get(key, value);
                }
            }
            return metadata;
        }

        pub fn usesLogicalDocumentIdentity(self: *Txn) !bool {
            const owner = self.columnar_owner orelse self.portable_import_reader_owner orelse return false;
            if (!owner.documentLayoutEnabled()) return false;
            var scope = try self.openReadScope(self.alloc);
            defer scope.close();
            return scope.usesLogicalDocumentIdentity();
        }

        pub fn documentManifestCache(self: *Txn) ?*document_payload.ManifestCache {
            const owner = self.columnar_owner orelse self.portable_import_reader_owner orelse return null;
            return owner.documentManifestCache();
        }

        pub fn getPhysical(self: *Txn, key: []const u8) ![]const u8 {
            if (self.write) |*write| return try write.get(key);
            if (self.probe) |*probe| return try probe.get(key);
            if (self.current_scan != null) return error.Unsupported;
            return try self.read.?.get(key);
        }

        /// Raw immutable bytes for a caller holding their native lifetime
        /// proof. This does not materialize current document/asset references.
        pub fn getLeasedPhysical(self: *Txn, key: []const u8) ![]const u8 {
            if (self.probe) |*probe| return probe.getLeased(key);
            return self.getPhysical(key);
        }

        /// Short-lived value lease, released when this transaction aborts.
        /// Immutable LSM bytes may be pinned rather than copied.
        pub fn getLeased(self: *Txn, key: []const u8) ![]const u8 {
            if (self.probe) |*probe| {
                const value = try probe.getLeased(key);
                if (internal_keys.isStoredDocumentRowKey(key) and document_payload.isManifest(value)) return self.get(key);
                if (self.payload_session) |session| return try session.get(key, value);
                if (artifact_payload.isEmbeddingKey(key) and artifact_payload.isReference(value))
                    return error.VectorMigrationReadEpochChanged;
                return value;
            }
            return try self.get(key);
        }

        /// Block-scoped values from this exact snapshot. Close scopes before
        /// aborting the transaction (which also owns the portable-import fence).
        pub fn openReadScope(self: *Txn, alloc: Allocator) !backend_erased.ReadScope {
            var scope = if (self.read) |*read| try read.openReadScope(alloc) else if (self.write) |*write| try write.openReadScope(alloc) else return error.ReadOnly;
            const owner = self.columnar_owner orelse self.portable_import_reader_owner;
            scope.logical_documents = if (owner) |store| store.documentLayoutEnabled() else false;
            if (scope.logical_documents) scope.document_manifest_cache = .{ .context = owner.?, .get = DocStore.documentManifestCachePort };
            return scope;
        }

        pub fn getManySorted(self: *Txn, keys: []const []const u8, values: []?[]const u8) !void {
            try self.getManySortedPhysical(keys, values);
            const manifests = for (keys, values) |key, value| {
                if (internal_keys.isStoredDocumentRowKey(key) and value != null and document_payload.isManifest(value.?)) break true;
            } else false;
            if (manifests) {
                const a = self.documentAllocator();
                if (self.probe != null) {
                    const owner = self.portable_import_reader_owner orelse return error.Unsupported;
                    var pinned = try owner.beginReadTxn();
                    defer pinned.abort();
                    for (keys, values) |key, *value| {
                        const raw = pinned.get(key) catch |err| {
                            if (err == error.NotFound) {
                                value.* = null;
                                continue;
                            }
                            return err;
                        };
                        value.* = try a.dupe(u8, raw);
                    }
                    return;
                }
                for (values) |*value| if (value.*) |raw| {
                    value.* = try a.dupe(u8, raw);
                };
                for (keys, values) |key, *value| if (value.*) |raw| {
                    value.* = try document_payload.read(a, self, key, raw);
                };
            }
            if (self.payload_session) |session| {
                for (keys, values) |key, *value| if (value.*) |raw| {
                    value.* = try session.get(key, raw);
                };
            } else for (keys, values) |key, value| if (value) |raw| {
                if (artifact_payload.isEmbeddingKey(key) and artifact_payload.isReference(raw))
                    return error.VectorMigrationReadEpochChanged;
            };
        }

        pub fn getManySortedPhysical(self: *Txn, keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidArgument;
            @memset(values, null);

            if (self.write) |*write| {
                for (keys, 0..) |key, i| {
                    values[i] = write.get(key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                }
                return;
            }
            if (self.probe) |*probe| {
                return try probe.getManySorted(keys, values);
            }
            if (self.current_scan != null) return error.Unsupported;
            return try self.read.?.getManySorted(keys, values);
        }

        /// Fetch values without admitting their source data blocks to the LSM
        /// cache. Callers should use this only when they retain a decoded or
        /// otherwise more useful representation of every returned value.
        pub fn getManySortedTransient(self: *Txn, keys: []const []const u8, values: []?[]const u8) !void {
            for (keys) |key| if (internal_keys.isStoredDocumentRowKey(key)) return self.getManySorted(keys, values);
            if (self.probe) |*probe| {
                try probe.getManySortedWithBlockCacheAdmission(keys, values, .transient);
                if (self.payload_session) |session| {
                    for (keys, values) |key, *value| if (value.*) |raw| {
                        value.* = try session.get(key, raw);
                    };
                } else for (keys, values) |key, value| if (value) |raw| {
                    if (artifact_payload.isEmbeddingKey(key) and artifact_payload.isReference(raw))
                        return error.VectorMigrationReadEpochChanged;
                };
                return;
            }
            return try self.getManySorted(keys, values);
        }

        /// Keep physical references in the transaction arena. Resolve payloads
        /// into bounded scratch and visit them only after source locks retire.
        pub fn consumeDenseManySorted(self: *Txn, alloc: Allocator, keys: []const []const u8, values: []?[]const u8, dims: usize, sink: artifact_payload.DenseSink) !artifact_payload.DenseReadStats {
            const session = self.payload_session orelse return error.Unsupported;
            const started = platform_time.monotonicNs();
            if (self.probe) |*probe| {
                try probe.getManySortedWithBlockCacheAdmission(keys, values, .transient);
            } else {
                try self.getManySortedPhysical(keys, values);
            }
            const primary_done = platform_time.monotonicNs();
            var stats = try session.consumeDenseMany(alloc, keys, values, dims, sink);
            stats.primary_lookup_ns += primary_done -| started;
            stats.payload_consume_ns += platform_time.monotonicNs() -| primary_done;
            return stats;
        }

        pub fn put(self: *Txn, key: []const u8, value: []const u8) anyerror!void {
            const prepared = if (self.columnar_owner != null and self.columnar_owner.?.documentLayoutEnabled() and internal_keys.isPrimaryDocumentKey(key))
                if (self.artifact_inputs.participant) |participant|
                    if (participant.document_preparations) |documents| documents.find(documents.ptr, key) else null
                else
                    null
            else
                null;
            return self.putPreparedDocument(key, value, prepared);
        }
        pub fn putPreparedDocument(self: *Txn, key: []const u8, value: []const u8, prepared: ?*const document_payload.PreparedWrite) anyerror!void {
            const qualified = if (self.columnar_owner != null and self.columnar_owner.?.documentLayoutEnabled() and prepared != null and try prepared.?.qualifies(self, key, value)) prepared else null;
            if (self.columnar_owner != null and self.columnar_owner.?.documentLayoutEnabled()) try document_payload.requireReaderFence(self, key, value);
            if (self.columnar_owner) |owner| if (owner.documentLayoutEnabled()) document_payload.observeFormatWrite(&owner.document_values_possible, key, value);
            try updateGraphOwningTable(self, key, value);
            try updateGraphEndpointCleanupAdmission(self, self.alloc, key, value);
            if (internal_keys.isInternalUserKey(key) and !self.graph_directory_checked) {
                self.graph_directory_checked = true;
                try initializeEmptyGraphIncomingDirectory(self);
            }
            if (internal_keys.isGraphRetirementKey(key)) {
                try applyGraphRetirement(self, self.alloc, key, value, &self.graph_retirements_maybe);
            }
            try requireGraphEndpointWritable(self, self.alloc, key, value);
            if (try graphRelationshipRetiredCached(self, self.alloc, key, &self.graph_retirements_maybe)) return;

            try maintainGraphIncoming(self, self.alloc, key, value, false, false);
            if (self.columnar_owner) |owner| owner.configureDocumentCapture(&self.document_revisions);
            if (self.document_revisions.logical_commitments and internal_keys.isPrimaryDocumentKey(key) and value.len >= self.document_revisions.logical_minimum_bytes) {
                self.document_revisions.logical_commitments = try document_payload.admitForWrite(self.alloc, &self.write.?, &self.document_revisions.document_format, self.document_revisions.minimum_reader_format, value.len, !self.columnar_owner.?.document_payload_prototype);
                // Publish the hint before the physical root can commit. A
                // reader pins its root first and still checks that root's floor.
                if (self.document_revisions.logical_commitments) self.columnar_owner.?.document_native_active.store(true, .release);
            }
            if (qualified) |receipt| try self.document_revisions.touchMetadataPatch(self.alloc, key, receipt.patch) else try self.document_revisions.touchValueWithReader(self.alloc, self, key, value);
            try self.document_revisions.recordValidation(self.alloc, key, value, self.artifact_inputs.participant);
            try self.artifact_footprint.touch(key, value);
            try self.range_mutation.touch(self, key);
            try self.artifact_inputs.touch(self.alloc, self, key, value);
            try self.retained.touch(self.alloc, self, key, internal_keys.isStoredDocumentRowKey(key), if (self.columnar_owner) |owner| &owner.retained_effects_cache else null);
            if (self.mutation_capture) |capture| try capture.touch(key);
            try self.markColumnarDirty(key, value);
            try self.invalidateColumns(key);

            errdefer self.document_revisions.poisoned = true;
            var document: ?[]u8 = if (qualified) |receipt| try self.alloc.dupe(u8, receipt.patch.manifest) else null;
            defer if (document) |bytes| self.alloc.free(bytes);
            if (qualified) |receipt| {
                _ = self.columnar_owner.?.document_prepared_stages.fetchAdd(1, .monotonic);
                self.document_revisions.markManifest(key, document);
                self.document_revisions.document_payload_stats.manifest_bytes += receipt.patch.manifest.len;
                self.document_revisions.document_payload_stats.blob_bytes_reused += receipt.patch.reused_payload_bytes;
            }
            if (qualified == null and self.columnar_owner != null and self.columnar_owner.?.documentLayoutEnabled() and !self.columnar_owner.?.document_payload_inline) if (self.document_revisions.sourcePublication(key)) |publication| {
                const removed_before = self.document_revisions.document_payload_stats.current_values_removed;
                if (try document_payload.shouldStage(self, &self.columnar_owner.?.document_values_possible, publication, value.len))
                    document = try document_payload.stage(self.alloc, &self.write.?, key, value, publication, &self.document_revisions.document_format, &self.document_revisions.document_payload_stats);
                if (document != null) self.columnar_owner.?.document_values_possible.store(2, .release);
                if (document == null and self.document_revisions.document_payload_stats.current_values_removed != removed_before) self.document_revisions.notePriorExternal(key);
                self.document_revisions.markManifest(key, document);
            };
            const stored = document orelse if (self.payload_session) |session| try session.put(key, value) else value;
            try self.write.?.put(key, if (self.payload_session) |session| if (document == null) session.primaryValue(value, stored) else stored else stored);
            if (self.payload_session) |session| try session.recordOwnership(&self.write.?, key, stored);
        }

        /// Bounded storage-level metadata patch. Like put(), callers own derived
        /// index maintenance; this is not a shortcut around DB batch admission.
        /// Unsupported layouts fall back before any mutation is staged.
        pub fn patchMetadata(self: *Txn, key: []const u8, updates: []const document_payload.MetadataUpdate) !void {
            if (self.write == null) return error.ReadOnly;
            if (self.columnar_owner == null or !self.columnar_owner.?.documentLayoutEnabled()) return error.Unsupported;
            if (!internal_keys.isStoredDocumentRowKey(key) or self.artifact_inputs.participant != null) return error.Unsupported;
            const a = self.documentAllocator();
            const revisions = @import("document_mutation_revision.zig");
            const revision_key = revisions.keyForPhysical(key).?;
            const current = self.document_revisions.sourcePublication(key);
            var proof = if (current) |value| value.proof else (try revisions.loadContent(self, key)) orelse return error.Unsupported;
            if (proof.algorithm != .logical_json_v1) return error.Unsupported;
            const raw = try a.dupe(u8, try self.getPhysical(key));
            if (!document_payload.isManifest(raw)) return error.Unsupported;
            proof.manifest = true;
            const manifest = try document_payload.Manifest.decode(a, raw);
            defer a.free(manifest.parts);
            const navigation = try manifest.navigation(a, proof.kind);
            defer if (navigation) |value| value.deinit(a);
            const paths = if (navigation) |value| value.directory else if (current) |value| value.directory orelse return error.Unsupported else try a.dupe(u8, try self.getPhysical(&@import("document_projection.zig").keyForRevision(revision_key)));
            const identities = if (navigation) |value| value.certificate else if (current) |value| value.identities orelse return error.Unsupported else try a.dupe(u8, try self.getPhysical(&@import("source_value_identity.zig").keyForRevision(revision_key)));
            const patch = try document_payload.prepareMetadataPatch(self.alloc, raw, proof, paths, identities, updates);
            defer patch.deinit(self.alloc);
            errdefer self.document_revisions.poisoned = true;
            try self.document_revisions.touchMetadataPatch(self.alloc, key, patch);
            try self.artifact_footprint.touch(key, patch.manifest);
            try self.range_mutation.touch(self, key);
            try self.artifact_inputs.touch(self.alloc, self, key, patch.manifest);
            try self.retained.touch(self.alloc, self, key, true, if (self.columnar_owner) |owner| &owner.retained_effects_cache else null);
            if (self.mutation_capture) |capture| try capture.touch(key);
            try self.write.?.put(key, patch.manifest);
            self.document_revisions.document_payload_stats.manifest_bytes += patch.manifest.len;
            self.document_revisions.document_payload_stats.blob_bytes_reused += patch.reused_payload_bytes;
        }

        pub fn delete(self: *Txn, key: []const u8) anyerror!void {
            if (self.columnar_owner) |owner| owner.configureDocumentCapture(&self.document_revisions);
            if (self.columnar_owner != null and self.columnar_owner.?.documentLayoutEnabled()) if (self.write) |*write| {
                errdefer self.document_revisions.poisoned = true;
                if (try document_payload.valuesPossible(self, &self.columnar_owner.?.document_values_possible)) try document_payload.retire(self.alloc, write, key, &self.document_revisions.document_payload_stats);
            };
            try updateGraphEndpointCleanupAdmission(self, self.alloc, key, null);
            try removeGraphRetirementReference(self, self.alloc, key, &self.graph_retirements_maybe);
            if (try internal_keys.graphIncomingKeyAlloc(self.alloc, key)) |incoming| {
                defer self.alloc.free(incoming);
                self.delete(incoming) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
            }
            try self.document_revisions.touchValue(self.alloc, key, null);
            try self.artifact_footprint.touch(key, null);
            try self.range_mutation.touch(self, key);
            try self.artifact_inputs.touch(self.alloc, self, key, null);
            try self.retained.touch(self.alloc, self, key, internal_keys.isStoredDocumentRowKey(key), if (self.columnar_owner) |owner| &owner.retained_effects_cache else null);
            if (self.mutation_capture) |capture| try capture.touch(key);
            try self.markColumnarDirty(key, null);
            try self.invalidateColumns(key);

            try self.write.?.delete(key);
            if (self.payload_session) |session| {
                if (artifact_payload.isEmbeddingKey(key)) session.reference_mutated = true;
                try session.recordOwnership(&self.write.?, key, null);
            }
        }

        fn markColumnarDirty(self: *Txn, key: []const u8, value: ?[]const u8) anyerror!void {
            if (!internal_keys.isRelationalRowKey(key)) return;
            const dirty = try internal_keys.relationalColumnarDirtyKeyAlloc(self.alloc, key);
            defer self.alloc.free(dirty);
            const token = try columnarMutationToken(self, &self.columnar_mutation);
            try self.put(dirty, &internal_keys.relationalColumnarDirtyRecord(token, if (value) |bytes| bytes.len else 0));
        }

        fn invalidateColumns(self: *Txn, key: []const u8) anyerror!void {
            if (self.columns_invalidated or !internal_keys.invalidatesRelationalColumns(key)) return;
            self.delete(internal_keys.relational_columnar_manifest_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.columns_invalidated = true;
        }

        pub fn openCursor(self: *Txn) !CursorAdapter {
            return try self.openCursorAdapter();
        }

        fn openCursorAdapter(self: *Txn) !CursorAdapter {
            return self.openDocumentCursorAdapter(false);
        }
        fn openDocumentCursorAdapter(self: *Txn, physical_vectors: bool) !CursorAdapter {
            var cursor_adapter = try self.openPhysicalCursorAdapter();
            errdefer cursor_adapter.close();
            const owner = self.columnar_owner orelse self.portable_import_reader_owner;
            const documents = if (owner) |store| store.documentLayoutEnabled() else false;
            if (!documents) {
                if (physical_vectors) return cursor_adapter;
                return wrapPayloadCursor(self.alloc, cursor_adapter, self.payload_session, self.current_scan != null or self.probe != null, null, null, null, false);
            }
            return try wrapPayloadCursor(self.alloc, cursor_adapter, if (physical_vectors) null else self.payload_session, self.current_scan != null or self.probe != null, if (self.read) |*read| try read.forkRead() else null, self.write, null, physical_vectors);
        }

        pub fn hasPrefix(self: *Txn, prefix: []const u8) !bool {
            if (self.write) |*write| return try write.hasPrefix(prefix);
            var cursor = try self.openPhysicalKeyCursorAdapter();
            defer cursor.close();
            const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
            return std.mem.startsWith(u8, row.key, prefix);
        }

        pub fn openPhysicalKeyCursorAdapter(self: *Txn) !CursorAdapter {
            var cursor = try self.openPhysicalCursorAdapter();
            cursor.setKeysOnly();
            return cursor;
        }

        pub fn openPhysicalCursorAdapter(self: *Txn) !CursorAdapter {
            if (self.write) |*write| return try write.openCursor();
            if (self.current_scan) |*current_scan| return try current_scan.openCursor();
            if (self.probe != null) return error.Unsupported;
            return try self.read.?.openCursor();
        }

        pub fn readAdapter(self: *Txn) ReadAdapter {
            return ReadAdapter.init(self);
        }

        pub fn writeAdapter(self: *Txn) WriteAdapter {
            return WriteAdapter.init(self);
        }
    };

    pub const Batch = struct {
        range_mutation: @import("range_protection.zig").Mutation = .{},
        retained: retained_effects.Capture = .{},
        artifact_inputs: @import("artifact_input_capture.zig").Capture = .{},
        artifact_footprint: @import("artifact_footprint.zig").Capture = .{},
        document_revisions: @import("document_mutation_revision.zig").Capture = .{},
        columnar_owner: ?*DocStore = null,
        payload_session: ?*artifact_payload.Session = null,
        document_arena: ?std.heap.ArenaAllocator = null,
        alloc: Allocator,
        columns_invalidated: bool = false,
        columnar_mutation: ?internal_keys.ColumnarMutationToken = null,
        graph_directory_checked: bool = false,
        graph_retirements_maybe: ?bool = null,
        unordered_bulk_append_puts: bool = false,
        runtime: ?backend_erased.Batch = null,

        pub fn setCommitParticipant(self: *Batch, participant: @import("commit_participant.zig").Participant) !void {
            try self.artifact_inputs.attach(participant);
        }

        pub const BatchTxn = struct {
            range_mutation: *@import("range_protection.zig").Mutation,
            retained: *retained_effects.Capture,
            artifact_inputs: *@import("artifact_input_capture.zig").Capture,
            artifact_footprint: *@import("artifact_footprint.zig").Capture,
            document_revisions: *@import("document_mutation_revision.zig").Capture,
            retained_cache: ?*std.atomic.Value(u8) = null,
            document_payload_prototype: bool = false,
            document_payload_inline: bool = false,
            automatic_document_layout: bool = false,
            document_layout_owner: ?*DocStore = null,
            minimum_reader_format: u32 = 26,
            logical_minimum_bytes: usize = @import("document_projection.zig").minimum_bytes,
            document_values_possible: ?*std.atomic.Value(u8) = null,
            document_prepared_stages: ?*std.atomic.Value(u64) = null,
            payload_session: ?*artifact_payload.Session = null,
            document_arena: *?std.heap.ArenaAllocator,
            alloc: Allocator,
            columns_invalidated: *bool,
            columnar_mutation: *?internal_keys.ColumnarMutationToken,
            graph_directory_checked: *bool,
            graph_retirements_maybe: *?bool,
            unordered_bulk_append_puts: bool = false,
            runtime: ?*backend_erased.Batch = null,

            pub fn consumeDenseManySorted(self: @This(), alloc: Allocator, keys: []const []const u8, values: []?[]const u8, dims: usize, sink: artifact_payload.DenseSink) !artifact_payload.DenseReadStats {
                const session = self.payload_session orelse return error.Unsupported;
                if (keys.len != values.len) return error.InvalidArgument;
                const started = platform_time.monotonicNs();
                try self.runtime.?.getManySorted(keys, values);
                const primary_done = platform_time.monotonicNs();
                var stats = try session.consumeDenseMany(alloc, keys, values, dims, sink);
                stats.primary_lookup_ns += primary_done -| started;
                stats.payload_consume_ns += platform_time.monotonicNs() -| primary_done;
                return stats;
            }

            fn documentAllocator(self: @This()) Allocator {
                if (self.document_arena.* == null) self.document_arena.* = .init(@import("antfly_platform").allocator.reclaimingScratch(self.alloc));
                return self.document_arena.*.?.allocator();
            }
            pub fn poison(self: @This()) void {
                self.retained.poisoned = true;
                self.document_revisions.poisoned = true;
            }

            pub fn getPhysical(self: @This(), key: []const u8) ![]const u8 {
                return self.runtime.?.get(key);
            }
            pub fn get(self: @This(), key: []const u8) ![]const u8 {
                const value = try self.runtime.?.get(key);
                if (internal_keys.isStoredDocumentRowKey(key) and document_payload.isManifest(value)) return document_payload.read(self.documentAllocator(), &self, key, value);
                if (self.payload_session) |session| return try session.get(key, value);
                // A live probe admitted before migration may observe a later
                // reference. Retry with a new source lease; never leak its encoding
                // or acquire a lease after reading a potentially retired reference.
                if (artifact_payload.isEmbeddingKey(key) and artifact_payload.isReference(value))
                    return error.VectorMigrationReadEpochChanged;
                return value;
            }

            pub fn getManySorted(self: @This(), keys: []const []const u8, values: []?[]const u8) !void {
                if (keys.len != values.len) return error.InvalidArgument;
                @memset(values, null);

                try self.runtime.?.getManySorted(keys, values);
                const manifests = for (keys, values) |key, value| {
                    if (internal_keys.isStoredDocumentRowKey(key) and value != null and document_payload.isManifest(value.?)) break true;
                } else false;
                if (manifests) {
                    const a = self.documentAllocator();
                    for (values) |*value| if (value.*) |raw| {
                        value.* = try a.dupe(u8, raw);
                    };
                    for (keys, values) |key, *value| if (value.*) |raw| {
                        value.* = try document_payload.read(a, &self, key, raw);
                    };
                }
                if (self.payload_session) |session| {
                    for (keys, values) |key, *value| if (value.*) |raw| {
                        value.* = try session.get(key, raw);
                    };
                }
            }

            pub fn put(self: @This(), key: []const u8, value: []const u8) anyerror!void {
                const prepared = if (self.document_payload_prototype and internal_keys.isPrimaryDocumentKey(key))
                    if (self.artifact_inputs.participant) |participant|
                        if (participant.document_preparations) |documents| documents.find(documents.ptr, key) else null
                    else
                        null
                else
                    null;
                return self.putPreparedDocument(key, value, prepared);
            }
            pub fn putPreparedDocument(self: @This(), key: []const u8, value: []const u8, prepared: ?*const document_payload.PreparedWrite) anyerror!void {
                const qualified = if (self.document_payload_prototype and prepared != null and try prepared.?.qualifies(self, key, value)) prepared else null;
                if (self.document_payload_prototype) try document_payload.requireReaderFence(self, key, value);
                if (self.document_payload_prototype) document_payload.observeFormatWrite(self.document_values_possible, key, value);
                try updateGraphOwningTable(self, key, value);
                try updateGraphEndpointCleanupAdmission(self, self.alloc, key, value);
                if (internal_keys.isInternalUserKey(key) and !self.graph_directory_checked.*) {
                    self.graph_directory_checked.* = true;
                    try initializeEmptyGraphIncomingDirectory(self);
                }
                if (internal_keys.isGraphRetirementKey(key)) {
                    try applyGraphRetirement(self, self.alloc, key, value, self.graph_retirements_maybe);
                }
                try requireGraphEndpointWritable(self, self.alloc, key, value);
                if (try graphRelationshipRetiredCached(self, self.alloc, key, self.graph_retirements_maybe)) return;

                try maintainGraphIncoming(self, self.alloc, key, value, false, false);
                self.document_revisions.logical_commitments = self.document_payload_prototype;
                self.document_revisions.logical_minimum_bytes = self.logical_minimum_bytes;
                self.document_revisions.minimum_reader_format = self.minimum_reader_format;
                if (self.document_payload_prototype and internal_keys.isPrimaryDocumentKey(key) and value.len >= self.logical_minimum_bytes) {
                    self.document_revisions.logical_commitments = try document_payload.admitForWrite(self.alloc, self.runtime.?, &self.document_revisions.document_format, self.minimum_reader_format, value.len, self.automatic_document_layout);
                    if (self.document_revisions.logical_commitments) if (self.document_layout_owner) |owner| owner.document_native_active.store(true, .release);
                }
                if (qualified) |receipt| try self.document_revisions.touchMetadataPatch(self.alloc, key, receipt.patch) else try self.document_revisions.touchValueWithReader(self.alloc, self, key, value);
                try self.document_revisions.recordValidation(self.alloc, key, value, self.artifact_inputs.participant);
                try self.artifact_footprint.touch(key, value);
                try self.range_mutation.touch(self, key);
                try self.artifact_inputs.touch(self.alloc, self, key, value);
                try self.retained.touch(self.alloc, self, key, internal_keys.isStoredSourceRowKey(key), self.retained_cache);
                try self.markColumnarDirty(key, value);
                try self.invalidateColumns(key);

                errdefer self.document_revisions.poisoned = true;
                var document: ?[]u8 = if (qualified) |receipt| try self.alloc.dupe(u8, receipt.patch.manifest) else null;
                defer if (document) |bytes| self.alloc.free(bytes);
                if (qualified) |receipt| {
                    if (self.document_prepared_stages) |counter| _ = counter.fetchAdd(1, .monotonic);
                    self.document_revisions.markManifest(key, document);
                    self.document_revisions.document_payload_stats.manifest_bytes += receipt.patch.manifest.len;
                    self.document_revisions.document_payload_stats.blob_bytes_reused += receipt.patch.reused_payload_bytes;
                }
                if (qualified == null and self.document_payload_prototype and !self.document_payload_inline) if (self.document_revisions.sourcePublication(key)) |publication| {
                    const removed_before = self.document_revisions.document_payload_stats.current_values_removed;
                    if (try document_payload.shouldStage(self, self.document_values_possible, publication, value.len))
                        document = try document_payload.stage(self.alloc, self.runtime.?, key, value, publication, &self.document_revisions.document_format, &self.document_revisions.document_payload_stats);
                    if (document != null) if (self.document_values_possible) |hint| hint.store(2, .release);
                    if (document == null and self.document_revisions.document_payload_stats.current_values_removed != removed_before) self.document_revisions.notePriorExternal(key);
                    self.document_revisions.markManifest(key, document);
                };
                const stored = document orelse if (self.payload_session) |session| try session.put(key, value) else value;
                try self.runtime.?.put(key, if (self.payload_session) |session| if (document == null) session.primaryValue(value, stored) else stored else stored);
                if (self.payload_session) |session| try session.recordOwnership(self.runtime.?, key, stored);
            }

            pub fn appendPut(self: @This(), key: []const u8, value: []const u8) anyerror!void {
                return self.appendPutChecked(key, value, false);
            }

            fn appendPutChecked(self: @This(), key: []const u8, value: []const u8, retirement_checked: bool) anyerror!void {
                self.document_revisions.logical_commitments = self.document_payload_prototype;
                self.document_revisions.logical_minimum_bytes = self.logical_minimum_bytes;
                self.document_revisions.minimum_reader_format = self.minimum_reader_format;
                if (self.document_payload_prototype and internal_keys.isPrimaryDocumentKey(key) and value.len >= self.logical_minimum_bytes) {
                    self.document_revisions.logical_commitments = try document_payload.admitForWrite(self.alloc, self.runtime.?, &self.document_revisions.document_format, self.minimum_reader_format, value.len, self.automatic_document_layout);
                    if (self.document_revisions.logical_commitments) if (self.document_layout_owner) |owner| owner.document_native_active.store(true, .release);
                }
                if (self.document_payload_prototype) try document_payload.requireReaderFence(self, key, value);
                if (self.document_payload_prototype) document_payload.observeFormatWrite(self.document_values_possible, key, value);
                if (std.mem.startsWith(u8, key, internal_keys.graph_endpoint_cleanup_prefix) and self.unordered_bulk_append_puts) return error.Unsupported;
                try updateGraphOwningTable(self, key, value);
                try updateGraphEndpointCleanupAdmission(self, self.alloc, key, value);
                if (internal_keys.isInternalUserKey(key) and !self.graph_directory_checked.*) {
                    self.graph_directory_checked.* = true;
                    try initializeEmptyGraphIncomingDirectory(self);
                }
                if (internal_keys.isGraphRetirementKey(key)) {
                    if (self.unordered_bulk_append_puts) return error.Unsupported;
                    try applyGraphRetirement(self, self.alloc, key, value, self.graph_retirements_maybe);
                }
                // Read-dependent filtering cannot stay in the unordered append
                // arena: fall back once instead of draining it for every edge.
                if (!retirement_checked and self.unordered_bulk_append_puts and internal_keys.isGraphEdgeArtifactKey(key) and
                    try graphRetirementsPresentCached(self, self.graph_retirements_maybe)) return error.Unsupported;
                try requireGraphEndpointWritable(self, self.alloc, key, value);
                if (!retirement_checked and try graphRelationshipRetiredCached(self, self.alloc, key, self.graph_retirements_maybe)) return;

                try maintainGraphIncoming(self, self.alloc, key, value, true, retirement_checked);
                if (self.unordered_bulk_append_puts) try self.document_revisions.touchValue(self.alloc, key, value) else try self.document_revisions.touchValueWithReader(self.alloc, self, key, value);
                try self.document_revisions.recordValidation(self.alloc, key, value, self.artifact_inputs.participant);
                try self.artifact_footprint.touch(key, value);
                // Active tracking requires point updates for bucket counters.
                // Restore bulk writers publish into a fresh identity; they may
                // defer activation until their unordered import is complete.
                try self.range_mutation.touch(self, key);
                try self.artifact_inputs.touch(self.alloc, self, key, value);
                try self.retained.touch(self.alloc, self, key, internal_keys.isStoredSourceRowKey(key), self.retained_cache);
                if (self.unordered_bulk_append_puts and internal_keys.isRelationalRowKey(key)) {
                    // Keep auxiliary records in the bulk arena too. A regular
                    // put drains that arena into a sorted mutable map, causing
                    // quadratic insertion when dirty keys precede every row.
                    const dirty = try internal_keys.relationalColumnarDirtyKeyAlloc(self.alloc, key);
                    defer self.alloc.free(dirty);
                    const token = try columnarMutationToken(self, self.columnar_mutation);
                    try self.runtime.?.appendPut(dirty, &internal_keys.relationalColumnarDirtyRecord(token, value.len));
                } else {
                    try self.markColumnarDirty(key, value);
                }
                try self.invalidateColumns(key);

                errdefer self.document_revisions.poisoned = true;
                var document: ?[]u8 = null;
                defer if (document) |bytes| self.alloc.free(bytes);
                if (self.document_payload_prototype and !self.document_payload_inline and !self.unordered_bulk_append_puts) if (self.document_revisions.sourcePublication(key)) |publication| {
                    const removed_before = self.document_revisions.document_payload_stats.current_values_removed;
                    if (try document_payload.shouldStage(self, self.document_values_possible, publication, value.len))
                        document = try document_payload.stage(self.alloc, self.runtime.?, key, value, publication, &self.document_revisions.document_format, &self.document_revisions.document_payload_stats);
                    if (document != null) if (self.document_values_possible) |hint| hint.store(2, .release);
                    if (document == null and self.document_revisions.document_payload_stats.current_values_removed != removed_before) self.document_revisions.notePriorExternal(key);
                    self.document_revisions.markManifest(key, document);
                };
                const stored = document orelse if (self.payload_session) |session| try session.put(key, value) else value;
                if (document != null) try self.runtime.?.put(key, stored) else try self.runtime.?.appendPut(key, if (self.payload_session) |session| session.primaryValue(value, stored) else stored);
                if (self.payload_session) |session| try session.recordOwnership(self.runtime.?, key, stored);
            }

            pub fn delete(self: @This(), key: []const u8) anyerror!void {
                self.document_revisions.logical_commitments = self.document_payload_prototype;
                self.document_revisions.logical_minimum_bytes = self.logical_minimum_bytes;
                self.document_revisions.minimum_reader_format = self.minimum_reader_format;
                errdefer self.document_revisions.poisoned = true;
                if (self.document_payload_prototype and try document_payload.valuesPossible(self, self.document_values_possible)) try document_payload.retire(self.alloc, self.runtime.?, key, &self.document_revisions.document_payload_stats);
                try updateGraphEndpointCleanupAdmission(self, self.alloc, key, null);
                try removeGraphRetirementReference(self, self.alloc, key, self.graph_retirements_maybe);
                if (try internal_keys.graphIncomingKeyAlloc(self.alloc, key)) |incoming| {
                    defer self.alloc.free(incoming);
                    self.delete(incoming) catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                }
                try self.document_revisions.touchValue(self.alloc, key, null);
                try self.artifact_footprint.touch(key, null);
                try self.range_mutation.touch(self, key);
                try self.artifact_inputs.touch(self.alloc, self, key, null);
                try self.retained.touch(self.alloc, self, key, internal_keys.isStoredSourceRowKey(key), self.retained_cache);
                try self.markColumnarDirty(key, null);
                try self.invalidateColumns(key);

                try self.runtime.?.delete(key);
                if (self.payload_session) |session| {
                    if (artifact_payload.isEmbeddingKey(key)) session.reference_mutated = true;
                    try session.recordOwnership(self.runtime.?, key, null);
                }
            }

            fn markColumnarDirty(self: @This(), key: []const u8, value: ?[]const u8) anyerror!void {
                if (!internal_keys.isRelationalRowKey(key)) return;
                const dirty = try internal_keys.relationalColumnarDirtyKeyAlloc(self.alloc, key);
                defer self.alloc.free(dirty);
                const token = try columnarMutationToken(self, self.columnar_mutation);
                try self.put(dirty, &internal_keys.relationalColumnarDirtyRecord(token, if (value) |bytes| bytes.len else 0));
            }

            fn invalidateColumns(self: @This(), key: []const u8) anyerror!void {
                if (self.columns_invalidated.* or !internal_keys.invalidatesRelationalColumns(key)) return;
                self.delete(internal_keys.relational_columnar_manifest_key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.columns_invalidated.* = true;
            }

            pub fn hasPrefix(self: @This(), prefix: []const u8) !bool {
                return try self.runtime.?.hasPrefix(prefix);
            }

            pub fn openCursor(self: @This()) !backend_erased.Cursor {
                var physical = try self.runtime.?.openCursor();
                errdefer physical.close();
                return try wrapPayloadCursor(self.alloc, physical, self.payload_session, false, null, null, if (self.document_payload_prototype) self.runtime.?.* else null, false);
            }

            pub fn setReplayOpaque(self: @This(), sequence: u64, payload: []const u8) !void {
                try writeReplayEntries(self.alloc, self, sequence, payload);
            }
        };

        pub const Adapter = backend_adapter.Batch(Batch, .{
            .abort = abort,
            .commit = commit,
            .get = batchGet,
            .put = batchPut,
            .delete = batchDelete,
        });

        /// Preserve the transactional ordered view through runtime erasure.
        /// Prepare-time journal accounting uses one prefix seek only when its
        /// durable aggregate has not yet been initialized.
        pub fn openCursor(self: *Batch) !backend_erased.Cursor {
            return self.asTxn().openCursor();
        }

        pub fn abort(self: *Batch) void {
            if (self.document_arena) |*arena| arena.deinit();
            self.retained.deinit(self.alloc);
            self.artifact_inputs.deinit(self.alloc);
            self.document_revisions.deinit(self.alloc);
            const payload_session = self.payload_session;
            defer if (payload_session) |session| session.release();

            if (self.runtime) |*runtime| {
                runtime.abort();
            }
            self.* = undefined;
        }

        pub fn commit(self: *Batch) !void {
            if (self.columnar_owner) |owner| _ = owner.query_generation_revision.fetchAdd(1, .release);
            errdefer self.retained.poisoned = true;
            errdefer if (self.columnar_owner) |owner| owner.committed_replay_next_cached.store(0, .release);
            try self.retained.stage(self.alloc, self.asTxn());
            {
                self.retained.staging = true;
                defer self.retained.staging = false;
                try self.artifact_inputs.stage(self.asTxn(), self.retained.staged);
                try self.artifact_footprint.stage(self.asTxn());
                try self.document_revisions.stage(self.asTxn());
            }
            const columnar_owner = if (self.columnar_mutation != null or self.columns_invalidated) self.columnar_owner else null;
            const source_owner = self.columnar_owner;
            if (source_owner) |owner| owner.source_certificates.begin(&self.document_revisions.documents);
            defer if (source_owner) |owner| owner.source_certificates.end();
            const payload_session = self.payload_session;
            if (payload_session) |session| try session.stageReferenceEpoch(self);
            if (payload_session) |session| try session.prepareCommit();
            if (payload_session) |session| session.primary_commit_attempted = true;
            if (self.runtime) |*runtime| {
                try runtime.commit();
            }
            if (payload_session) |session| {
                session.committed = true;
                session.release();
            }
            if (self.document_arena) |*arena| arena.deinit();
            self.retained.deinit(self.alloc);
            self.artifact_inputs.deinit(self.alloc);
            self.document_revisions.deinit(self.alloc);
            self.* = undefined;
            if (columnar_owner) |owner| _ = owner.columnar_revision.fetchAdd(1, .release);
        }

        pub fn asTxn(self: *Batch) BatchTxn {
            return .{
                .range_mutation = &self.range_mutation,
                .retained = &self.retained,
                .artifact_inputs = &self.artifact_inputs,
                .artifact_footprint = &self.artifact_footprint,
                .document_revisions = &self.document_revisions,
                .retained_cache = if (self.columnar_owner) |owner| &owner.retained_effects_cache else null,
                .document_payload_prototype = if (self.columnar_owner) |owner| owner.documentMigrationEnabled() else false,
                .document_layout_owner = self.columnar_owner,
                .automatic_document_layout = if (self.columnar_owner) |owner| !owner.document_payload_prototype else false,
                .minimum_reader_format = if (self.columnar_owner) |owner| owner.documentMinimumReader() else 26,
                .logical_minimum_bytes = @import("document_projection.zig").minimum_bytes,
                .document_payload_inline = if (self.columnar_owner) |owner| owner.document_payload_inline else false,
                .document_values_possible = if (self.columnar_owner) |owner| &owner.document_values_possible else null,
                .document_prepared_stages = if (self.columnar_owner) |owner| &owner.document_prepared_stages else null,
                .payload_session = self.payload_session,
                .document_arena = &self.document_arena,
                .alloc = self.alloc,
                .columns_invalidated = &self.columns_invalidated,
                .columnar_mutation = &self.columnar_mutation,
                .graph_directory_checked = &self.graph_directory_checked,
                .graph_retirements_maybe = &self.graph_retirements_maybe,
                .unordered_bulk_append_puts = self.unordered_bulk_append_puts,
                .runtime = if (self.runtime) |*runtime| runtime else null,
            };
        }

        pub fn get(self: *Batch, key: []const u8) ![]const u8 {
            return try self.asTxn().get(key);
        }

        pub fn put(self: *Batch, key: []const u8, value: []const u8) !void {
            try self.asTxn().put(key, value);
        }

        pub fn delete(self: *Batch, key: []const u8) !void {
            try self.asTxn().delete(key);
        }

        pub fn setReplayOpaque(self: *Batch, sequence: u64, payload: []const u8) !void {
            try self.asTxn().setReplayOpaque(sequence, payload);
        }

        fn batchGet(self: *Batch, key: []const u8) ![]const u8 {
            return try self.get(key);
        }

        fn batchPut(self: *Batch, key: []const u8, value: []const u8) !void {
            try self.put(key, value);
        }

        fn batchDelete(self: *Batch, key: []const u8) !void {
            try self.delete(key);
        }

        pub fn adapter(self: *Batch) Adapter {
            return Adapter.init(self);
        }
    };

    pub fn open(alloc: Allocator, path: [*:0]const u8, opts: DocStoreOptions) !DocStore {
        var backend = try lsm_backend.BackendHandle.open(alloc, std.mem.span(path), .{
            .backend = .{
                .read_only = opts.read_only,
                .create_if_missing = !opts.read_only,
            },
            .wal_enabled = !opts.read_only,
        });
        errdefer backend.close();
        const runtime_store = try backend.backend.runtimeStore(alloc, .{});
        return .{
            .alloc = alloc,
            .runtime_store = runtime_store,
            .owns_runtime_store = true,
            .owned_lsm_backend = backend,
            .replay_index_state = .init(replay_index_unknown),
            .next_replay_sequence_cached = .init(0),
        };
    }

    pub fn openRuntime(alloc: Allocator, store: anytype) !DocStore {
        const runtime_store = try initRuntimeStore(alloc, store);
        return .{
            .alloc = alloc,
            .runtime_store = runtime_store.store,
            .owns_runtime_store = runtime_store.owned,
            .owned_lsm_backend = null,
            .replay_index_state = .init(replay_index_unknown),
            .next_replay_sequence_cached = .init(0),
        };
    }

    fn documentManifestCachePort(context: *anyopaque) ?*document_payload.ManifestCache {
        const self: *DocStore = @ptrCast(@alignCast(context));
        return self.documentManifestCache();
    }
    fn documentManifestCache(self: *DocStore) ?*document_payload.ManifestCache {
        if (!self.documentLayoutEnabled()) return null;
        if (self.document_manifest_cache.load(.acquire)) |cache| return cache;
        self.lockPayloadPolicy();
        defer self.unlockPayloadPolicy();
        if (self.document_manifest_cache.load(.acquire) == null) {
            const a = @import("antfly_platform").allocator.reclaimingScratch(self.alloc);
            const cache = a.create(document_payload.ManifestCache) catch return null;
            cache.* = .init(a);
            self.document_manifest_cache.store(cache, .release);
        }
        return self.document_manifest_cache.load(.acquire);
    }
    pub fn close(self: *DocStore) void {
        if (self.document_manifest_cache.load(.acquire)) |cache| {
            cache.deinit();
            @import("antfly_platform").allocator.reclaimingScratch(self.alloc).destroy(cache);
        }
        self.source_certificates.deinit(self.alloc);
        if (self.owns_runtime_store) self.runtime_store.deinit();
        if (self.owned_lsm_backend) |*backend| backend.close();
        self.* = undefined;
    }

    fn backendCapabilities(self: *DocStore) backend_types.Capabilities {
        return self.runtime_store.capabilities();
    }

    pub fn backendStore(self: *DocStore) BackendStore {
        return BackendStore.init(self);
    }

    pub fn sync(self: *DocStore, force: bool) !void {
        try self.ensurePortableImportOperational();
        try self.runtime_store.sync(force);
    }

    pub fn syncReplayState(self: *DocStore) !void {
        try self.ensurePortableImportOperational();
        try self.runtime_store.syncReplayState();
    }

    const PayloadCursor = struct {
        physical: backend_erased.Cursor,
        session: ?*artifact_payload.Session,
        arena: std.heap.ArenaAllocator,
        reader: ?backend_erased.ReadTxn = null,
        writer: ?backend_erased.WriteTxn = null,
        batch: ?backend_erased.Batch = null,
        physical_vectors: bool = false,
        keys_only: bool = false,
        pub fn getPhysical(self: *@This(), key: []const u8) ![]const u8 {
            if (self.reader) |*reader| return reader.get(key);
            if (self.writer) |*writer| return writer.get(key);
            if (self.batch) |*batch| return batch.get(key);
            return error.Unsupported;
        }
        pub fn setKeysOnly(self: *@This()) void {
            self.keys_only = true;
            self.physical.setKeysOnly();
        }
        pub fn close(self: *@This()) void {
            self.physical.close();
            if (self.reader) |*reader| reader.abort();
            self.arena.deinit();
            if (self.session) |session| session.release();
        }
        fn resolve(self: *@This(), entry: ?backend_erased.Entry) !?backend_erased.Entry {
            _ = self.arena.reset(.retain_capacity);
            const value = entry orelse return null;
            // Some adapters have no key-only operation. Preserve its logical
            // contract locally without decoding a returned native descriptor.
            if (self.keys_only) return .{ .key = value.key, .value = &.{} };
            if (document_payload.isManifest(value.value) and internal_keys.isStoredDocumentRowKey(value.key)) {
                if (self.reader == null and self.writer == null and self.batch == null) return error.Unsupported;
                const key = try self.arena.allocator().dupe(u8, value.key);
                return .{ .key = key, .value = try document_payload.read(self.arena.allocator(), self, key, value.value) };
            }
            if (self.session) |session| return .{ .key = value.key, .value = try session.getAlloc(self.arena.allocator(), value.key, value.value) };
            if (!self.physical_vectors and artifact_payload.isEmbeddingKey(value.key) and artifact_payload.isReference(value.value))
                return error.VectorMigrationReadEpochChanged;
            return value;
        }
        fn visible(self: *@This(), entry: ?backend_erased.Entry, reverse: bool) !?backend_erased.Entry {
            var current = entry;
            while (current) |value| {
                // Private ownership records stay private independently of the
                // payload-reader configuration. Ordinary rows avoid every
                // private-prefix probe on this common scan path.
                if (!std.mem.startsWith(u8, value.key, "\x00\x00__metadata__:")) break;
                const prefix = document_payload.ownership.privatePrefix(value.key) orelse @import("index_source_claim.zig").privatePrefix(value.key) orelse break;
                if (reverse) {
                    current = if (std.mem.eql(u8, value.key, prefix)) try self.physical.prev() else try self.physical.seekAtOrBefore(prefix);
                } else {
                    var upper: [128]u8 = undefined;
                    @memcpy(upper[0..prefix.len], prefix);
                    upper[prefix.len - 1] += 1;
                    current = try self.physical.seekAtOrAfter(upper[0..prefix.len]);
                }
            }
            return self.resolve(current);
        }
        pub fn first(self: *@This()) !?backend_erased.Entry {
            return self.visible(try self.physical.first(), false);
        }
        pub fn last(self: *@This()) !?backend_erased.Entry {
            return self.visible(try self.physical.last(), true);
        }
        pub fn next(self: *@This()) !?backend_erased.Entry {
            return self.visible(try self.physical.next(), false);
        }
        pub fn prev(self: *@This()) !?backend_erased.Entry {
            return self.visible(try self.physical.prev(), true);
        }
        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_erased.Entry {
            return self.visible(try self.physical.seekAtOrAfter(key), false);
        }
        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_erased.Entry {
            return self.visible(try self.physical.seekAtOrBefore(key), true);
        }
        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            self.physical.setUpperBound(upper);
        }
    };

    fn wrapPayloadCursor(alloc: Allocator, physical: backend_erased.Cursor, session: ?*artifact_payload.Session, live: bool, read: ?backend_erased.ReadTxn, write: ?backend_erased.WriteTxn, batch: ?backend_erased.Batch, physical_vectors: bool) !backend_erased.Cursor {
        // Logical cursors always enforce metadata privacy, including during
        // startup/import before a native payload reader is configured.
        _ = live;
        var reader = read;
        errdefer if (reader) |*owned| owned.abort();
        if (session) |owner| owner.retain();
        errdefer if (session) |owner| owner.release();
        return try backend_erased.cursorFrom(alloc, PayloadCursor{
            .physical = physical,
            .session = session,
            .reader = reader,
            .writer = write,
            .batch = batch,
            .physical_vectors = physical_vectors,
            .arena = std.heap.ArenaAllocator.init(alloc),
        });
    }

    fn lockPayloadPolicy(self: *DocStore) void {
        // This synchronous ABI has no borrowed task Io. Use the non-spawning
        // synchronization Io, as the backend's other synchronous gates do.
        // Admission can wait for the backend lock: spinning here starves its
        // holder under concurrent query hydration and CPU quotas.
        self.payload_policy_mutex.lockUncancelable(payloadPolicyIo());
    }

    fn payloadPolicyIo() std.Io {
        return if (builtin.os.tag == .freestanding) .failing else std.Io.Threaded.global_single_threaded.io();
    }

    fn unlockPayloadPolicy(self: *DocStore) void {
        self.payload_policy_mutex.unlock(payloadPolicyIo());
    }

    /// DB apply admission excludes writers. Reader admission holds this mutex
    /// until its primary view and source lease have both been captured.
    pub fn configurePayloadPolicy(self: *DocStore, store: ?artifact_payload.Store, capture_inline: bool, migration_allowance: ?u64) void {
        self.lockPayloadPolicy();
        defer self.unlockPayloadPolicy();
        self.payload_store = store;
        self.payload_capture_inline = capture_inline;
        self.payload_migration_allowance = migration_allowance;
    }

    fn createPayloadSession(self: *DocStore) !?*artifact_payload.Session {
        if (self.payload_recovery_required.load(.acquire)) return error.VectorMigrationRecoveryRequired;
        const store = self.payload_store orelse return null;
        const session = try artifact_payload.Session.create(self.alloc, store);
        session.capture_inline = self.payload_capture_inline;
        session.migration_allowance = self.payload_migration_allowance;
        return session;
    }

    fn createWritePayloadSession(self: *DocStore) !?*artifact_payload.Session {
        // A Lite writer slot may be held by a dense-index transaction whose
        // vector callback opens a primary probe. Do not hold the policy mutex
        // while waiting for that slot: probes need it to capture their own
        // payload session before the writer can finish.
        self.lockPayloadPolicy();
        defer self.unlockPayloadPolicy();
        return try self.createPayloadSession();
    }

    pub fn prefersImmutablePointReads(self: *DocStore, point_lookups: usize) bool {
        return self.runtime_store.prefersImmutablePointReads(point_lookups);
    }

    pub fn beginReadTxn(self: *DocStore) !Txn {
        return try self.beginReadTxnWithBlockCacheAdmission(.retain);
    }

    /// Runtime namespaces authenticate persisted blocks before exposing values.
    pub fn valuesAreAuthenticated(_: *const DocStore) bool {
        return true;
    }

    pub fn beginReadTxnWithBlockCacheAdmission(
        self: *DocStore,
        admission: backend_types.Namespace.BlockCacheAdmission,
    ) !Txn {
        try self.acquirePortableImportReader();
        errdefer self.releasePortableImportReader();
        var txn = try self.beginReadTxnUncheckedWithBlockCacheAdmission(admission);
        txn.portable_import_reader_owner = self;
        return txn;
    }

    fn beginReadTxnUnchecked(self: *DocStore) !Txn {
        return try self.beginReadTxnUncheckedWithBlockCacheAdmission(.retain);
    }

    fn beginReadTxnUncheckedWithBlockCacheAdmission(
        self: *DocStore,
        admission: backend_types.Namespace.BlockCacheAdmission,
    ) !Txn {
        self.lockPayloadPolicy();
        defer self.unlockPayloadPolicy();
        const payload_session = try self.createPayloadSession();
        errdefer if (payload_session) |session| session.release();
        return .{
            .payload_session = payload_session,
            .alloc = self.alloc,
            .read = try self.runtime_store.beginReadWithBlockCacheAdmission(admission),
        };
    }

    /// Read a sorted set of keys from one committed view without cloning the
    /// full mutable LSM state. The returned lease owns `values`; abort it after
    /// consuming them. Only this batch is snapshot-consistent: subsequent get
    /// calls on a live probe may see later commits. Backends without an atomic
    /// point-batch capability use their ordinary read snapshot instead.
    pub fn readManyConsistent(self: *DocStore, keys: []const []const u8, values: []?[]const u8) !Txn {
        var txn = try self.beginProbeTxn();
        if (txn.probe) |probe| {
            if (!probe.vtable.get_many_sorted_is_atomic) {
                txn.abort();
                txn = try self.beginReadTxn();
            }
        }
        errdefer txn.abort();
        try txn.getManySorted(keys, values);
        return txn;
    }

    /// Open a current-tip probe transaction for hot single-writer point reads.
    ///
    /// This reads the live mutable view without cloning an LSM snapshot.
    pub fn beginProbeTxn(self: *DocStore) !Txn {
        return try self.beginProbeTxnWithBlockCacheAdmission(.retain);
    }

    pub fn beginProbeTxnWithBlockCacheAdmission(
        self: *DocStore,
        admission: backend_types.Namespace.BlockCacheAdmission,
    ) !Txn {
        try self.acquirePortableImportReader();
        errdefer self.releasePortableImportReader();
        self.lockPayloadPolicy();
        defer self.unlockPayloadPolicy();
        const payload_session = try self.createPayloadSession();
        errdefer if (payload_session) |session| session.release();
        var txn: Txn = .{
            .payload_session = payload_session,
            .alloc = self.alloc,
            .probe = try self.runtime_store.beginProbeWithBlockCacheAdmission(admission),
        };
        txn.portable_import_reader_owner = self;
        return txn;
    }

    /// Open a current-tip replay scan transaction for ordered replay walks.
    ///
    /// This follows append-only lanes without widening the point-probe API.
    pub fn beginCurrentScanTxn(self: *DocStore) !Txn {
        try self.acquirePortableImportReader();
        errdefer self.releasePortableImportReader();
        self.lockPayloadPolicy();
        defer self.unlockPayloadPolicy();
        const payload_session = try self.createPayloadSession();
        errdefer if (payload_session) |session| session.release();
        var txn: Txn = .{
            .payload_session = payload_session,
            .alloc = self.alloc,
            .current_scan = try self.runtime_store.beginCurrentScan(),
        };
        txn.portable_import_reader_owner = self;
        return txn;
    }

    /// Open one stable replay-lane generation. Runtime LSM implementations
    /// narrow the mutable snapshot to this lane; callers may retain the
    /// transaction and cursor while consuming several bounded replay chunks.
    pub fn beginReplayLaneScanTxn(self: *DocStore, kind_ordinal: u8, from_sequence: u64) !Txn {
        if (!(try self.hasReplayEntries())) return error.ReplayIndexUnavailable;
        try self.acquirePortableImportReader();
        errdefer self.releasePortableImportReader();
        self.lockPayloadPolicy();
        defer self.unlockPayloadPolicy();
        const payload_session = try self.createPayloadSession();
        errdefer if (payload_session) |session| session.release();
        var txn: Txn = .{
            .payload_session = payload_session,
            .alloc = self.alloc,
            .current_scan = try self.runtime_store.beginReplayLaneScan(kind_ordinal, from_sequence),
        };
        txn.portable_import_reader_owner = self;
        return txn;
    }

    pub const GraphEndpointCleanupPage = struct {
        guards: []const @import("graph_cleanup_contract.zig").Guard = &.{},
        alloc: Allocator,
        writes: []KVPair,
        replay_writes: []KVPair = &.{},
        deletes: []const []const u8,
        inspected: usize,
        bytes: usize,
        pub fn deinit(self: *@This()) void {
            for (self.guards) |guard| self.alloc.free(guard.endpoint);
            if (self.guards.len != 0) self.alloc.free(self.guards);
            for (self.writes) |row| {
                self.alloc.free(row.key);
                self.alloc.free(row.value);
            }
            for (self.replay_writes) |row| {
                self.alloc.free(row.key);
                self.alloc.free(row.value);
            }
            if (self.replay_writes.len != 0) self.alloc.free(self.replay_writes);
            for (self.deletes) |key| self.alloc.free(key);
            self.alloc.free(self.writes);
            self.alloc.free(self.deletes);
            self.* = undefined;
        }
    };

    pub fn graphIncomingDirectoryReady(self: *DocStore) !bool {
        var read = try self.beginReadTxn();
        defer read.abort();
        _ = read.get(internal_keys.graph_incoming_ready_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        return true;
    }

    fn hasPrefixTxn(txn: *Txn, prefix: []const u8) !bool {
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        return std.mem.startsWith(u8, row.key, prefix);
    }

    pub fn hasGraphEndpointCleanup(self: *DocStore) !bool {
        var read = try self.beginReadTxn();
        defer read.abort();
        return try hasPrefixTxn(&read, internal_keys.graph_endpoint_cleanup_prefix) or try hasPrefixTxn(&read, internal_keys.graph_owner_replay_prefix);
    }

    /// Empty endpoint jobs do not change the graph and need not fence reads.
    /// This transactionally maintained summary avoids scanning the job queue on
    /// every traversal. Old queues without a summary remain conservatively fenced.
    pub fn graphEndpointCleanupBlocksReads(self: *DocStore) !bool {
        var read = try self.beginReadTxn();
        defer read.abort();
        if (try hasPrefixTxn(&read, internal_keys.graph_owner_replay_prefix)) return true;
        const count = read.get(internal_keys.graph_endpoint_cleanup_count_key) catch |err| switch (err) {
            error.NotFound => {
                var cursor = try read.openPhysicalCursorAdapter();
                defer cursor.close();
                const row = (try cursor.seekAtOrAfter(internal_keys.graph_endpoint_cleanup_prefix)) orelse return false;
                return std.mem.startsWith(u8, row.key, internal_keys.graph_endpoint_cleanup_prefix);
            },
            else => return err,
        };
        if (count.len != 8) return error.InvalidGraphSegment;
        return std.mem.readInt(u64, count[0..8], .little) != 0;
    }

    /// Borrowed transaction check keeps every insertion path, including bulk
    /// append, behind the same durable target fence. Facts have no directory
    /// entry and deliberately retain their independent document lifecycle.
    /// The local lifecycle directory contains only targets in this physical
    /// namespace. Explicit foreign tags never participate in local deletion.
    fn localGraphInlineTarget(txn: anytype, alloc: Allocator, artifact: []const u8, value: []const u8) !?[]const u8 {
        const target = internal_keys.graphInlineTargetComponent(artifact) orelse return null;
        const codec = @import("db/enrichment/artifact_codec.zig");
        if (std.mem.startsWith(u8, value, &codec.magic)) {
            const edge = try codec.decodeGraphEdgeBorrowed(value);
            var scratch = @import("../graph/metadata_tables.zig").Scratch.init(alloc, null);
            defer scratch.deinit();
            const source_table = try scratch.table(edge.metadata_json, "source_table");
            const target_table = try scratch.table(edge.metadata_json, "target_table");
            if (source_table != null or target_table != null) {
                const here = txn.get(internal_keys.graph_owning_table_key) catch |err| switch (err) {
                    error.NotFound => return null,
                    else => return err,
                };
                // Equal document keys in a foreign source table do not make
                // an independently owned fact into a source-owned inline edge.
                if (!@import("../graph/metadata_tables.zig").inlineEndpointsAreLocal(source_table, target_table, here)) return null;
            }
        }
        return target;
    }

    /// Recheck a planned endpoint deletion against the current routing value.
    /// A same-identity write may have moved the relationship to a foreign table.
    pub fn graphRelationshipLocallyOwned(txn: anytype, alloc: Allocator, artifact: []const u8) !bool {
        const value = txn.get(artifact) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        return try localGraphInlineTarget(txn, alloc, artifact, value) != null;
    }

    fn maintainGraphIncoming(txn: anytype, alloc: Allocator, artifact: []const u8, value: []const u8, append: bool, incoming_checked: bool) !void {
        const physical_key = (try internal_keys.graphIncomingKeyAlloc(alloc, artifact)) orelse return;
        defer alloc.free(physical_key);
        const local_target = try localGraphInlineTarget(txn, alloc, artifact, value);
        if (local_target != null) {
            if (comptime @hasDecl(switch (@typeInfo(@TypeOf(txn))) {
                .pointer => |pointer| pointer.child,
                else => @TypeOf(txn),
            }, "appendPut")) {
                if (append) try txn.appendPut(physical_key, artifact) else try txn.put(physical_key, artifact);
            } else try txn.put(physical_key, artifact);
        } else {
            if (comptime @hasField(switch (@typeInfo(@TypeOf(txn))) {
                .pointer => |pointer| pointer.child,
                else => @TypeOf(txn),
            }, "unordered_bulk_append_puts")) {
                if (append and txn.unordered_bulk_append_puts) {
                    // The sorted bulk preflight proved that no prior local
                    // membership needs deleting before these fresh appends.
                    if (incoming_checked) return;
                    return error.Unsupported;
                }
            }
            txn.delete(physical_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }

    fn updateGraphOwningTable(txn: anytype, key: []const u8, value: []const u8) !void {
        if (!std.mem.eql(u8, key, internal_keys.graph_owning_table_key)) return;
        const previous = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous) |name| if (std.mem.eql(u8, name, value)) return;
        try txn.put(internal_keys.graph_directory_reset_key, &.{0});
        for ([_][]const u8{ internal_keys.graph_incoming_ready_key, internal_keys.graph_incoming_cursor_key, internal_keys.graph_endpoint_cleanup_count_key }) |metadata_key| {
            txn.delete(metadata_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }

    fn requireGraphEndpointWritable(txn: anytype, alloc: Allocator, artifact: []const u8, value: []const u8) !void {
        const target = (try localGraphInlineTarget(txn, alloc, artifact, value)) orelse return;
        const endpoint = try internal_keys.decodeBodyAlloc(alloc, target[0 .. target.len - 2]);
        defer alloc.free(endpoint);
        const key = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, endpoint);
        defer alloc.free(key);
        _ = txn.get(key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        return error.IntegrityTopologyBusy;
    }

    /// Planning reads only a bounded page. Applying the returned afterimages
    /// must be ordered with ordinary writes (DB apply lock / Raft). Successful
    /// retirements remove their own directory inputs atomically, so restarting
    /// at the prefix is both a durable cursor and an idempotent retry.
    pub fn prepareGraphEndpointCleanupPage(self: *DocStore, alloc: Allocator) !?GraphEndpointCleanupPage {
        const owner_only = blk: {
            var read = try self.beginReadTxn();
            defer read.abort();
            break :blk !try hasPrefixTxn(&read, internal_keys.graph_endpoint_cleanup_prefix) and try hasPrefixTxn(&read, internal_keys.graph_owner_replay_prefix);
        };
        // Owner revival needs only its own primary prefixes. An unrelated
        // incoming-directory upgrade must not delay its bounded input replay.
        if (owner_only) return self.prepareGraphOwnerReplayPage(alloc);
        if (!try self.backfillGraphIncomingDirectoryPage()) return .{ .alloc = alloc, .writes = try alloc.alloc(KVPair, 0), .deletes = try alloc.alloc([]const u8, 0), .inspected = 0, .bytes = 0 };
        var guards = std.ArrayListUnmanaged(@import("graph_cleanup_contract.zig").Guard).empty;
        errdefer {
            for (guards.items) |guard| alloc.free(guard.endpoint);
            guards.deinit(alloc);
        }
        var writes = std.ArrayListUnmanaged(KVPair).empty;
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (writes.items) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
            }
            for (deletes.items) |key| alloc.free(key);
            writes.deinit(alloc);
            deletes.deinit(alloc);
        }
        var inspected: usize = 0;
        var bytes: usize = 0;
        {
            var read = try self.beginReadTxn();
            defer read.abort();
            var jobs = try read.openPhysicalCursorAdapter();
            defer jobs.close();
            var incoming = try read.openPhysicalCursorAdapter();
            defer incoming.close();
            var job_row = try jobs.seekAtOrAfter(internal_keys.graph_endpoint_cleanup_prefix);
            if (job_row == null or !std.mem.startsWith(u8, job_row.?.key, internal_keys.graph_endpoint_cleanup_prefix)) return self.prepareGraphOwnerReplayPage(alloc);
            while (job_row) |job| {
                if (!std.mem.startsWith(u8, job.key, internal_keys.graph_endpoint_cleanup_prefix)) break;
                if (inspected + deletes.items.len >= 256) break;
                const incarnation = try @import("graph_cleanup_contract.zig").decode(job.key, job.value);
                const expected_job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, incarnation.endpoint);
                defer alloc.free(expected_job);
                if (!std.mem.eql(u8, expected_job, job.key)) return error.InvalidGraphSegment;
                const job_key = try alloc.dupe(u8, job.key);
                defer alloc.free(job_key);
                const guard_bytes = @sizeOf(@import("graph_cleanup_contract.zig").Guard) +| incarnation.endpoint.len;
                if (inspected + deletes.items.len > 0 and bytes +| guard_bytes > 256 * 1024) break;
                bytes +|= guard_bytes;
                const owned_endpoint = try alloc.dupe(u8, incarnation.endpoint);
                guards.append(alloc, .{ .endpoint = owned_endpoint, .generation = incarnation.generation }) catch |err| {
                    alloc.free(owned_endpoint);
                    return err;
                };
                const prefix = try internal_keys.graphIncomingPrefixAlloc(alloc, incarnation.endpoint);
                defer alloc.free(prefix);
                var row = try incoming.seekAtOrAfter(prefix);
                while (row) |entry| {
                    if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                    if (inspected + deletes.items.len >= 256 or (inspected + deletes.items.len > 0 and bytes +| entry.key.len +| entry.value.len > 256 * 1024)) break;
                    const expected_entry = (try internal_keys.graphIncomingKeyAlloc(alloc, entry.value)) orelse return error.InvalidGraphSegment;
                    defer alloc.free(expected_entry);
                    if (!std.mem.eql(u8, expected_entry, entry.key)) return error.InvalidGraphSegment;
                    const retired = try internal_keys.graphRetirementKeyAlloc(alloc, entry.value);
                    const encoded = if (incarnation.generation != 0) @import("graph_cleanup_contract.zig").retirementValue(incarnation.generation) else undefined;
                    const value = alloc.dupe(u8, if (incarnation.generation != 0) &encoded else "1") catch |err| {
                        alloc.free(retired);
                        return err;
                    };
                    writes.append(alloc, .{ .key = retired, .value = value }) catch |err| {
                        alloc.free(retired);
                        alloc.free(value);
                        return err;
                    };
                    inspected += 1;
                    bytes +|= entry.key.len +| entry.value.len;
                    row = try incoming.next();
                }
                if (row != null and std.mem.startsWith(u8, row.?.key, prefix)) break;
                if (inspected + deletes.items.len >= 256 or (inspected + deletes.items.len > 0 and bytes +| job_key.len > 256 * 1024)) break;
                const owned_job = try alloc.dupe(u8, job_key);
                deletes.append(alloc, owned_job) catch |err| {
                    alloc.free(owned_job);
                    return err;
                };
                bytes +|= job_key.len;
                job_row = try jobs.next();
            }
        }
        const owned_writes = try writes.toOwnedSlice(alloc);
        errdefer {
            for (owned_writes) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
            }
            alloc.free(owned_writes);
        }
        const owned_guards = try guards.toOwnedSlice(alloc);
        errdefer {
            for (owned_guards) |guard| alloc.free(guard.endpoint);
            alloc.free(owned_guards);
        }
        return .{ .guards = owned_guards, .alloc = alloc, .writes = owned_writes, .deletes = try deletes.toOwnedSlice(alloc), .inspected = inspected, .bytes = bytes };
    }

    /// Exact maintenance query: explicitly completes directory migration.
    /// Write admission must use mayHaveGraphRetirements instead.
    /// Owner revival uses the same replicated maintenance lane as endpoint
    /// cleanup. Each page owns at most 255 records plus its checkpoint and
    /// releases its source snapshot before publication.
    fn prepareGraphOwnerReplayPage(self: *DocStore, alloc: Allocator) !?GraphEndpointCleanupPage {
        const contract = @import("graph_cleanup_contract.zig");
        var read = try self.beginReadTxn();
        defer read.abort();
        var jobs = try read.openPhysicalCursorAdapter();
        defer jobs.close();
        const row = (try jobs.seekAtOrAfter(internal_keys.graph_owner_replay_prefix)) orelse return null;
        if (!internal_keys.isGraphOwnerReplayJobKey(row.key)) return null;
        const job = try contract.decodeOwnerJob(row.key, row.value);
        var page: GraphEndpointCleanupPage = .{ .alloc = alloc, .writes = try alloc.alloc(KVPair, 0), .deletes = try alloc.alloc([]const u8, 0), .inspected = 0, .bytes = row.key.len + @sizeOf(contract.Guard) + job.owner.len };
        errdefer page.deinit();
        page.guards = blk: {
            const owner = try alloc.dupe(u8, job.owner);
            errdefer alloc.free(owner);
            const guards = try alloc.alloc(contract.Guard, 1);
            guards[0] = .{ .endpoint = owner, .generation = job.generation, .kind = .owner_replay, .checkpoint_digest = contract.checkpointDigest(row.value) };
            break :blk guards;
        };
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(alloc);
        errdefer for (deletes.items) |key| alloc.free(key);
        var replay = std.ArrayListUnmanaged(KVPair).empty;
        defer replay.deinit(alloc);
        errdefer for (replay.items) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        };
        const prefix = switch (job.phase) {
            .retirements => try internal_keys.graphRetirementPrefixAlloc(alloc, job.owner),
            .inputs => try internal_keys.artifactTypePrefixAlloc(alloc, job.owner, "asset"),
            .chunks => try internal_keys.artifactTypePrefixAlloc(alloc, job.owner, "chunk"),
            .resolutions => try internal_keys.artifactTypePrefixAlloc(alloc, job.owner, "resolution"),
        };
        defer alloc.free(prefix);
        if (job.cursor.len != 0 and !std.mem.startsWith(u8, job.cursor, prefix)) return error.InvalidGraphSegment;
        const start = if (job.cursor.len == 0) try alloc.dupe(u8, prefix) else try std.mem.concat(alloc, u8, &.{ job.cursor, &.{0} });
        defer alloc.free(start);
        var inputs = try read.openPhysicalCursorAdapter();
        defer inputs.close();
        var input = try inputs.seekAtOrAfter(start);
        var last: ?[]u8 = null;
        defer if (last) |key| alloc.free(key);
        while (input) |entry| {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            const value = if (job.phase != .retirements and contract.isReplayInput(entry.key)) try read.get(entry.key) else entry.value;
            const cost = entry.key.len +| (if (job.phase != .retirements and contract.isReplayInput(entry.key)) value.len else 0);
            if (page.inspected != 0 and (page.inspected >= 255 or page.bytes +| cost +| 21 +| job.owner.len +| entry.key.len > 256 * 1024)) break;
            if (job.phase == .retirements) {
                if (try contract.retirementGeneration(value) < job.generation) {
                    const key = try alloc.dupe(u8, entry.key);
                    deletes.append(alloc, key) catch |err| {
                        alloc.free(key);
                        return err;
                    };
                }
            } else if (contract.isReplayInput(entry.key)) {
                try replay.ensureUnusedCapacity(alloc, 1);
                const key = try alloc.dupe(u8, entry.key);
                errdefer alloc.free(key);
                replay.appendAssumeCapacity(.{ .key = key, .value = try alloc.dupe(u8, value) });
            }
            const next_cursor = try alloc.dupe(u8, entry.key);
            if (last) |key| alloc.free(key);
            last = next_cursor;
            page.inspected += 1;
            page.bytes +|= cost;
            input = try inputs.next();
        }
        const complete = input == null or !std.mem.startsWith(u8, input.?.key, prefix);
        if (complete and job.phase == .resolutions) {
            const key = try alloc.dupe(u8, row.key);
            deletes.append(alloc, key) catch |err| {
                alloc.free(key);
                return err;
            };
        } else {
            try replay.ensureUnusedCapacity(alloc, 1);
            const key = try alloc.dupe(u8, row.key);
            errdefer alloc.free(key);
            const value = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = job.owner, .generation = job.generation, .phase = if (complete) switch (job.phase) {
                .retirements => .inputs,
                .inputs => .chunks,
                .chunks => .resolutions,
                .resolutions => unreachable,
            } else job.phase, .cursor = if (complete) "" else last.? });
            replay.appendAssumeCapacity(.{ .key = key, .value = value });
            page.bytes +|= value.len;
        }
        alloc.free(page.deletes);
        page.deletes = try deletes.toOwnedSlice(alloc);
        page.replay_writes = try replay.toOwnedSlice(alloc);
        return page;
    }

    pub fn hasGraphRetirements(self: *DocStore) !bool {
        try self.ensureGraphIncomingDirectory();
        return self.mayHaveGraphRetirements();
    }

    /// Constant-time conservative admission hint. An incomplete directory
    /// returns true: callers check the authoritative owner/relationship prefix
    /// they need, rather than migrating unrelated primary rows on a write.
    /// Explicit migration/maintenance owns directory progress.
    pub fn mayHaveGraphRetirements(self: *DocStore) !bool {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        var maybe: ?bool = null;
        return graphRetirementsPresentCached(&txn, &maybe);
    }

    pub fn graphRelationshipRetired(self: *DocStore, key: []const u8) !bool {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        return graphRelationshipRetiredInTxn(&txn, self.alloc, key);
    }

    /// Physical range rewrites bypass per-key directory maintenance. Reset
    /// local derived state in bounded, restartable pages before rebuilding it.
    pub fn invalidateGraphDirectories(self: *DocStore) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        // A new physical rewrite restarts clearing even if a previous rebuild
        // was interrupted: its checkpoint described a different primary range.
        try txn.put(internal_keys.graph_directory_reset_key, &.{0});
        for ([_][]const u8{ internal_keys.graph_incoming_ready_key, internal_keys.graph_incoming_cursor_key, internal_keys.graph_endpoint_cleanup_count_key }) |key| {
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        try txn.commit();
    }

    pub fn rebuildGraphDirectories(self: *DocStore) !void {
        try self.invalidateGraphDirectories();
        try self.ensureGraphIncomingDirectory();
    }

    /// A durable page checkpoint limits temporary memory and writer occupancy.
    /// Ordinary writes maintain the directory throughout migration, including
    /// artifacts inserted behind the checkpoint. Read and publication share
    /// each page's writer transaction, so deletes cannot race the backfill.
    pub fn backfillGraphIncomingDirectoryPage(self: *DocStore) !bool {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        // Upgrade key-only directories before granting v3 cleanup admission.
        if (txn.get(internal_keys.graph_incoming_legacy_ready_key)) |_| {
            try txn.put(internal_keys.graph_directory_reset_key, &.{0});
            try txn.delete(internal_keys.graph_incoming_legacy_ready_key);
        } else |err| if (err != error.NotFound) return err;
        const reset = txn.get(internal_keys.graph_directory_reset_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (reset) |phase| {
            if (phase.len != 1 or phase[0] > 2) return error.InvalidGraphRetirement;
            if (phase[0] < 2) {
                const current = phase[0];
                const prefix = if (current == 0) internal_keys.graph_incoming_prefix else internal_keys.graph_retirement_ref_prefix;
                var arena = std.heap.ArenaAllocator.init(self.alloc);
                defer arena.deinit();
                const alloc = arena.allocator();
                var keys = std.ArrayListUnmanaged([]const u8).empty;
                var bytes: usize = 0;
                var done = false;
                {
                    var cursor = try txn.openPhysicalCursorAdapter();
                    defer cursor.close();
                    var entry = try cursor.seekAtOrAfter(prefix);
                    while (entry) |row| {
                        if (!std.mem.startsWith(u8, row.key, prefix)) break;
                        try keys.append(alloc, try alloc.dupe(u8, row.key));
                        bytes +|= row.key.len;
                        entry = try cursor.next();
                        if (keys.items.len >= 256 or bytes >= 256 * 1024) break;
                    }
                    done = if (entry) |row| !std.mem.startsWith(u8, row.key, prefix) else true;
                }
                for (keys.items) |key| try txn.delete(key);
                if (done) {
                    try txn.put(internal_keys.graph_directory_reset_key, &.{current + 1});
                    if (current == 1) try txn.put(internal_keys.graph_retirement_count_key, &(@as([8]u8, @splat(0))));
                }
                try txn.commit();
                return false;
            }
        }
        if (txn.get(internal_keys.graph_incoming_ready_key)) |_| {
            txn.abort();
            return true;
        } else |err| if (err != error.NotFound) return err;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const alloc = arena.allocator();
        const checkpoint = txn.get(internal_keys.graph_incoming_cursor_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const start = if (checkpoint) |key| try std.mem.concat(alloc, u8, &.{ key, &.{0} }) else &.{internal_keys.user_namespace};
        var entries = std.ArrayListUnmanaged(KVPair).empty;
        var last: ?[]const u8 = null;
        var done = false;
        var copied_bytes: usize = 0;
        {
            var cursor = try txn.openPhysicalCursorAdapter();
            defer cursor.close();
            cursor.setUpperBound(&.{internal_keys.user_namespace + 1});
            var entry = try cursor.seekAtOrAfter(start);
            var scanned: usize = 0;
            while (entry) |item| {
                last = try alloc.dupe(u8, item.key);
                copied_bytes +|= item.key.len;
                // Resolve external payloads through the transaction; routing
                // metadata is borrowed and never copied just to classify it.
                const graph_value = if (internal_keys.graphInlineTargetComponent(item.key) != null) try txn.get(item.key) else "";
                copied_bytes +|= graph_value.len;
                if (try localGraphInlineTarget(&txn, alloc, item.key, graph_value) != null) {
                    const key = (try internal_keys.graphIncomingKeyAlloc(alloc, item.key)).?;
                    const value = try alloc.dupe(u8, item.key);
                    try entries.append(alloc, .{ .key = key, .value = value });
                    copied_bytes +|= key.len +| value.len;
                }
                if (internal_keys.isGraphRetirementKey(item.key)) {
                    _ = try @import("graph_cleanup_contract.zig").retirementGeneration(item.value);
                    // Reusing marker puts makes restart and concurrent insert
                    // accounting idempotent through the reference directory.
                    try entries.append(alloc, .{ .key = last.?, .value = try alloc.dupe(u8, item.value) });
                }
                scanned += 1;
                entry = try cursor.next();
                // At most one oversized key is admitted in a page.
                if (scanned >= 256 or copied_bytes >= 256 * 1024) break;
            }
            done = entry == null;
        }
        for (entries.items) |item| try txn.put(item.key, item.value);
        if (done) {
            // Publish an explicit zero too: no marker has ever written a count.
            const count = try graphRetirementCount(&txn);
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, count, .little);
            try txn.put(internal_keys.graph_retirement_count_key, &bytes);
            try txn.put(internal_keys.graph_incoming_ready_key, "1");
            txn.delete(internal_keys.graph_directory_reset_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(internal_keys.graph_incoming_cursor_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        } else try txn.put(internal_keys.graph_incoming_cursor_key, last.?);
        try txn.commit();
        return done;
    }

    pub fn ensureGraphIncomingDirectory(self: *DocStore) !void {
        const ready = self.get(self.alloc, internal_keys.graph_incoming_ready_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (ready) |value| {
            self.alloc.free(value);
            return;
        }
        while (!try self.backfillGraphIncomingDirectoryPage()) {}
    }

    fn initializeEmptyGraphIncomingDirectory(txn: anytype) anyerror!void {
        if (txn.get(internal_keys.graph_directory_reset_key)) |_| return else |err| if (err != error.NotFound) return err;
        if (txn.get(internal_keys.graph_incoming_ready_key)) |_| return else |err| if (err != error.NotFound) return err;
        if (txn.get(internal_keys.graph_incoming_cursor_key)) |_| return else |err| if (err != error.NotFound) return err;
        const empty = blk: {
            var cursor = try txn.openCursor();
            defer cursor.close();
            cursor.setUpperBound(&.{internal_keys.user_namespace + 1});
            break :blk try cursor.seekAtOrAfter(&.{internal_keys.user_namespace}) == null;
        };
        if (empty) {
            try txn.put(internal_keys.graph_retirement_count_key, &(@as([8]u8, @splat(0))));
            try txn.put(internal_keys.graph_incoming_ready_key, "1");
        }
    }

    pub fn beginWriteTxn(self: *DocStore) !Txn {
        try self.ensurePortableImportOperational();
        const payload_session = try self.createWritePayloadSession();
        errdefer if (payload_session) |session| session.release();
        return .{
            .payload_session = payload_session,
            .alloc = self.alloc,
            .columnar_owner = self,
            .write = try self.runtime_store.beginWrite(),
        };
    }

    pub fn beginWriteBatch(self: *DocStore) !Batch {
        return try self.beginWriteBatchWithOptions(.{});
    }

    pub fn beginWriteBatchWithOptions(self: *DocStore, options: backend_types.BatchOptions) !Batch {
        try self.ensurePortableImportOperational();
        const payload_session = try self.createWritePayloadSession();
        errdefer if (payload_session) |session| session.release();
        return .{
            .payload_session = payload_session,
            .alloc = self.alloc,
            .columnar_owner = self,
            .runtime = try self.runtime_store.beginBatchWithOptions(options),
            .unordered_bulk_append_puts = options.mode == .bulk_ingest and self.runtime_store.capabilities().unordered_bulk_append_puts,
        };
    }

    pub fn beginBulkIngestSession(self: *DocStore) !void {
        try self.ensurePortableImportOperational();
        self.source_certificates.beginReplacement();
        errdefer self.source_certificates.end();
        try self.runtime_store.beginBulkIngestSession();
        _ = self.source_bulk_fences.fetchAdd(1, .monotonic);
    }

    pub fn finishBulkIngestSessionWithOptions(self: *DocStore, options: backend_types.BulkIngestFinishOptions) !void {
        try self.ensurePortableImportOperational();
        self.source_certificates.beginReplacement();
        defer self.source_certificates.end();
        try self.runtime_store.finishBulkIngestSessionWithOptions(options);
        self.releaseSourceBulkFence();
    }

    pub fn flushBufferedWritesWithOptions(self: *DocStore, options: backend_types.BulkIngestFinishOptions) !void {
        try self.ensurePortableImportOperational();
        self.source_certificates.beginReplacement();
        defer self.source_certificates.end();
        try self.runtime_store.flushBufferedWritesWithOptions(options);
    }

    pub fn abortBulkIngestSession(self: *DocStore) void {
        self.runtime_store.abortBulkIngestSession();
        self.releaseSourceBulkFence();
    }

    fn releaseSourceBulkFence(self: *DocStore) void {
        // Backends allow nested/concurrent bulk sessions. Retire exactly one
        // admission fence per completed or aborted session.
        var remaining = self.source_bulk_fences.load(.monotonic);
        while (remaining > 0) {
            if (self.source_bulk_fences.cmpxchgWeak(remaining, remaining - 1, .monotonic, .monotonic)) |actual| {
                remaining = actual;
            } else {
                self.source_certificates.end();
                return;
            }
        }
    }

    fn ensurePortableImportOperational(self: *const DocStore) !void {
        if (self.portable_import_recovery_required.load(.acquire)) {
            return error.PortableImportRecoveryRequired;
        }
    }

    fn acquirePortableImportReader(self: *DocStore) !void {
        try self.ensurePortableImportOperational();
        var state = self.portable_import_reader_state.load(.acquire);
        while (true) {
            if (state & portable_import_publication_bit != 0) return error.PortableImportPublicationInProgress;
            if (state == portable_import_reader_count_mask) return error.PortableImportReaderLimitReached;
            state = self.portable_import_reader_state.cmpxchgWeak(state, state + 1, .acquire, .monotonic) orelse break;
        }
        if (self.portable_import_recovery_required.load(.acquire)) {
            self.releasePortableImportReader();
            return error.PortableImportRecoveryRequired;
        }
    }

    fn releasePortableImportReader(self: *DocStore) void {
        const previous = self.portable_import_reader_state.fetchSub(1, .release);
        std.debug.assert(previous & portable_import_reader_count_mask > 0);
    }

    /// Enter the short reader-visible publication fence. DB's exclusive apply
    /// lock serializes publishers and ordinary writers; this atomic closes the
    /// remaining lock-free read path without penalizing reads with a mutex.
    pub fn beginPortableImportPublication(self: *DocStore, io: std.Io) !void {
        try self.ensurePortableImportOperational();
        var state = self.portable_import_reader_state.load(.acquire);
        while (true) {
            if (state & portable_import_publication_bit != 0) return error.PortableImportPublicationInProgress;
            state = self.portable_import_reader_state.cmpxchgWeak(
                state,
                state | portable_import_publication_bit,
                .acq_rel,
                .acquire,
            ) orelse break;
        }
        self.source_certificates.beginReplacement();
        while (self.portable_import_reader_state.load(.acquire) & portable_import_reader_count_mask != 0) {
            // Publication is rare, but an admitted reader may be an std.Io
            // fiber. Yield through that same scheduler rather than blocking an
            // executor thread while the reader drains.
            io.sleep(.fromNanoseconds(1), .awake) catch {};
        }
    }

    pub fn finishPortableImportPublication(self: *DocStore) void {
        self.source_certificates.end();
        self.retained_effects_cache.store(0, .release);
        self.document_values_possible.store(0, .release);
        std.debug.assert(self.portable_import_reader_state.load(.monotonic) == portable_import_publication_bit);
        self.portable_import_reader_state.store(0, .release);
    }

    pub fn portableImportPublicationInProgress(self: *const DocStore) bool {
        return self.portable_import_reader_state.load(.acquire) & portable_import_publication_bit != 0;
    }

    pub fn requirePortableImportRecovery(self: *DocStore) void {
        self.portable_import_recovery_required.store(true, .release);
    }

    pub fn portableImportRecoveryRequired(self: *const DocStore) bool {
        return self.portable_import_recovery_required.load(.acquire);
    }

    pub fn put(self: *DocStore, key: []const u8, value: []const u8) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(key, value);
        try txn.commit();
    }

    /// Get a value by key. Caller owns the returned slice.
    pub fn get(self: *DocStore, alloc: Allocator, key: []const u8) ![]u8 {
        // A positive hint only selects the I/O strategy. Always reload the row
        // under a fresh pinned snapshot, including after overwrite or deletion.
        if (internal_keys.isPrimaryDocumentKey(key)) if (self.document_manifest_cache.load(.acquire)) |cache| if (cache.knowsRow(key)) {
            var pinned = try self.beginReadTxn();
            defer pinned.abort();
            const raw = pinned.getPhysical(key) catch |err| {
                if (err == error.NotFound) cache.forgetRow(key);
                return err;
            };
            if (document_payload.isManifest(raw)) return @constCast(try document_payload.read(alloc, &pinned, key, raw));
            cache.forgetRow(key);
            return alloc.dupe(u8, raw);
        };
        // A one-key lookup has no multi-operation snapshot to preserve, and
        // the result is copied before this function returns. Use the live
        // point-probe contract so the runtime LSM does not clone its complete
        // mutable generation for every metadata/artifact lookup.
        var txn = try self.beginProbeTxn();
        defer txn.abort();
        if (internal_keys.isStoredDocumentRowKey(key)) {
            const raw = try txn.getPhysical(key);
            if (!document_payload.isManifest(raw)) return alloc.dupe(u8, raw);
            // A live probe cannot combine a manifest and values from different
            // commits. Retry under one snapshot and reconstruct directly into
            // the caller allocator, avoiding two additional full-body copies.
            var pinned = try self.beginReadTxn();
            defer pinned.abort();
            const stable = try pinned.getPhysical(key);
            if (!document_payload.isManifest(stable)) return alloc.dupe(u8, stable);
            return @constCast(try document_payload.read(alloc, &pinned, key, stable));
        }
        const val = try txn.get(key);
        return try alloc.dupe(u8, val);
    }

    /// Privileged point read paired with scanPortableImportRollbackWithContext.
    /// It keeps recovery-required fencing intact while allowing the publisher
    /// to consult its durable rollback marker and bounded baseline journal.
    pub fn getPortableImportRollback(self: *DocStore, alloc: Allocator, key: []const u8) ![]u8 {
        try self.ensurePortableImportOperational();
        var txn = try self.beginReadTxnUnchecked();
        defer txn.abort();
        const val = txn.get(key) catch |err| switch (err) {
            error.NotFound => return error.NotFound,
            else => return err,
        };
        return try alloc.dupe(u8, val);
    }

    pub fn delete(self: *DocStore, key: []const u8) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(key);
        try txn.commit();
    }

    /// Inspect committed artifact metadata without fetching external vectors.
    pub fn getArtifactMetadata(self: *DocStore, key: []const u8) !artifact_payload.Metadata {
        var txn = try self.beginProbeTxn();
        defer txn.abort();
        return try txn.getArtifactMetadata(key);
    }

    /// Atomic batch: apply all writes and deletes in a single transaction.
    pub fn putBatch(self: *DocStore, writes: []const KVPair, deletes: []const []const u8) !void {
        try self.putBatchWithReplay(null, writes, deletes, null);
    }

    pub const ReplayAppend = struct {
        sequence: u64,
        payload: []const u8,
    };

    pub const KeyPromotion = struct {
        source_key: []const u8,
        destination_key: []const u8,
    };

    /// Builds one value from the same write transaction used for key
    /// promotions. This lets callers serialize promotion source values while
    /// they are still borrowed and commit the derived write atomically.
    pub const TransactionalWriteBuilder = struct {
        ptr: *anyopaque,
        key: []const u8,
        build: *const fn (ptr: *anyopaque, alloc: Allocator, txn: *Batch.BatchTxn) anyerror![]u8,
    };

    /// Read-only predicate evaluated inside the exact write transaction that
    /// performs promotions and replay publication.
    pub const TransactionalGuard = struct {
        ptr: *anyopaque,
        validate: *const fn (ptr: *anyopaque, alloc: Allocator, txn: *Batch.BatchTxn) anyerror!void,
        validate_at_commit: ?*const fn (ptr: *anyopaque, alloc: Allocator, txn: *Batch.BatchTxn) anyerror!void = null,
    };

    /// A foreign rewrite may replace a previously local relationship. Detect
    /// those memberships with one sorted read before entering the append arena;
    /// fresh foreign bulk rows then avoid per-edge drains or a whole-batch fallback.
    fn graphBulkIncomingNeedsDelete(alloc: Allocator, txn: Batch.BatchTxn, writes: []const KVPair) !bool {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var keys = std.ArrayListUnmanaged([]const u8).empty;
        for (writes) |write| {
            if (!internal_keys.isGraphEdgeArtifactKey(write.key) or try localGraphInlineTarget(txn, scratch, write.key, write.value) != null) continue;
            if (try internal_keys.graphIncomingKeyAlloc(scratch, write.key)) |key| try keys.append(scratch, key);
        }
        if (keys.items.len == 0) return false;
        const Order = struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        };
        std.sort.pdq([]const u8, keys.items, {}, Order.less);
        const values = try scratch.alloc(?[]const u8, keys.items.len);
        try txn.getManySorted(keys.items, values);
        for (values) |value| if (value != null) return true;
        return false;
    }

    /// Resolve retirements before admitting any append entries. The writer
    /// transaction pins both the checks and sorted ingestion atomically.
    fn graphBulkRetirementMask(alloc: Allocator, txn: Batch.BatchTxn, writes: []const KVPair) !?[]bool {
        for (writes) |write| try requireGraphEndpointWritable(txn, alloc, write.key, write.value);
        if (!try graphRetirementsPresentCached(txn, txn.graph_retirements_maybe)) return null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const Candidate = struct {
            key: []const u8,
            index: usize,
            fn less(_: void, left: @This(), right: @This()) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        };
        var candidates = std.ArrayListUnmanaged(Candidate).empty;
        for (writes, 0..) |write, index| {
            if (!internal_keys.isGraphEdgeArtifactKey(write.key)) continue;
            try candidates.append(scratch, .{ .key = try internal_keys.graphRetirementKeyAlloc(scratch, write.key), .index = index });
        }
        if (candidates.items.len == 0) return null;
        std.sort.pdq(Candidate, candidates.items, {}, Candidate.less);
        const keys = try scratch.alloc([]const u8, candidates.items.len);
        const values = try scratch.alloc(?[]const u8, candidates.items.len);
        for (candidates.items, keys) |candidate, *key| key.* = candidate.key;
        try txn.getManySorted(keys, values);
        const mask = try alloc.alloc(bool, writes.len);
        errdefer alloc.free(mask);
        @memset(mask, false);
        for (candidates.items, values) |candidate, value| if (value) |stamp| {
            mask[candidate.index] = try retirementSuppressesRelationship(txn, scratch, writes[candidate.index].key, stamp);
        };
        return mask;
    }

    fn putBatchWithReplayOnceWithOptions(
        self: *DocStore,
        writes: []const KVPair,
        deletes: []const []const u8,
        replay: ?ReplayAppend,
        options: backend_types.BatchOptions,
    ) !void {
        return self.putBatchWithReplayOnceAndParticipant(writes, deletes, replay, options, null);
    }

    fn putBatchWithReplayOnceAndParticipant(
        self: *DocStore,
        writes: []const KVPair,
        deletes: []const []const u8,
        replay: ?ReplayAppend,
        options: backend_types.BatchOptions,
        participant: ?@import("commit_participant.zig").Participant,
    ) !void {
        const has_owner_job = blk: {
            for (writes) |write| if (internal_keys.isGraphOwnerReplayJobKey(write.key)) break :blk true;
            break :blk false;
        };
        const bulk_candidate = deletes.len == 0 and options.mode == .bulk_ingest and !has_owner_job;
        // The retirement mask reads candidate markers from this same writer
        // transaction. It remains authoritative during directory migration,
        // so bulk ingestion never needs a foreground whole-store backfill.
        var batch = try self.beginWriteBatchWithOptions(options);
        errdefer batch.abort();
        if (participant) |observer| try batch.setCommitParticipant(observer);
        var txn = batch.asTxn();
        const graph_bulk = bulk_candidate and !try graphBulkIncomingNeedsDelete(self.alloc, txn, writes);
        const retirement_mask = if (graph_bulk) try graphBulkRetirementMask(self.alloc, txn, writes) else null;
        defer if (retirement_mask) |mask| self.alloc.free(mask);
        for (deletes) |key| {
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {}, // ignore missing keys
                else => return err,
            };
        }
        var used_bulk_append = false;
        if (graph_bulk) {
            used_bulk_append = true;
            for (writes, 0..) |kv, index| {
                if (retirement_mask) |mask| if (mask[index]) continue;
                txn.appendPutChecked(kv.key, kv.value, true) catch |err| switch (err) {
                    error.Unsupported => {
                        used_bulk_append = false;
                        break;
                    },
                    else => return err,
                };
            }
        }
        if (!used_bulk_append) {
            for (writes) |kv| {
                try txn.put(kv.key, kv.value);
            }
        }
        if (replay) |entry| {
            try batch.setReplayOpaque(entry.sequence, entry.payload);
        }
        try batch.commit();
    }

    fn putBatchWithReplayOnce(self: *DocStore, writes: []const KVPair, deletes: []const []const u8, replay: ?ReplayAppend) !void {
        try self.putBatchWithReplayOnceWithOptions(writes, deletes, replay, .{});
    }

    pub fn putBatchWithReplayWithOptions(
        self: *DocStore,
        io: ?std.Io,
        writes: []const KVPair,
        deletes: []const []const u8,
        replay: ?ReplayAppend,
        options: backend_types.BatchOptions,
    ) !void {
        return self.putBatchWithReplayAndParticipant(io, writes, deletes, replay, options, null);
    }

    pub fn putBatchWithReplayAndParticipant(
        self: *DocStore,
        io: ?std.Io,
        writes: []const KVPair,
        deletes: []const []const u8,
        replay: ?ReplayAppend,
        options: backend_types.BatchOptions,
        participant: ?@import("commit_participant.zig").Participant,
    ) !void {
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            self.putBatchWithReplayOnceAndParticipant(writes, deletes, replay, options, participant) catch |err| switch (err) {
                error.WriterLocked => {
                    if (attempt >= writer_locked_retry_count) return err;
                    backoffWriterLockRetry(io);
                    continue;
                },
                else => return err,
            };
            if (replay) |entry| {
                self.markReplayIndexAvailable();
                self.observeCommittedReplaySequence(entry.sequence);
            }
            return;
        }
    }

    pub fn putBatchWithReplay(self: *DocStore, io: ?std.Io, writes: []const KVPair, deletes: []const []const u8, replay: ?ReplayAppend) !void {
        try self.putBatchWithReplayWithOptions(io, writes, deletes, replay, .{});
    }

    fn putBatchWithPromotionsAndReplayOnce(
        self: *DocStore,
        writes: []const KVPair,
        deletes: []const []const u8,
        promotions: []const KeyPromotion,
        replay: ?ReplayAppend,
        transactional_write: ?TransactionalWriteBuilder,
        transactional_guard: ?TransactionalGuard,
    ) !usize {
        var batch = try self.beginWriteBatchWithOptions(.{});
        errdefer batch.abort();
        var txn = batch.asTxn();
        if (transactional_guard) |guard| try guard.validate(guard.ptr, self.alloc, &txn);
        const built_value = if (transactional_write) |builder|
            try builder.build(builder.ptr, self.alloc, &txn)
        else
            null;
        defer if (built_value) |value| self.alloc.free(value);
        if (transactional_write) |builder| try txn.put(builder.key, built_value.?);
        var promoted_bytes: usize = 0;
        for (promotions) |promotion| {
            const value = txn.get(promotion.source_key) catch |err| switch (err) {
                error.NotFound => return error.InvalidKeyPromotion,
                else => return err,
            };
            promoted_bytes = std.math.add(usize, promoted_bytes, value.len) catch
                return error.KeyPromotionBytesOverflow;
            try txn.put(promotion.destination_key, value);
            try txn.delete(promotion.source_key);
        }
        for (deletes) |key| {
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        for (writes) |kv| try txn.put(kv.key, kv.value);
        if (replay) |entry| try batch.setReplayOpaque(entry.sequence, entry.payload);
        // Recheck immediately before commit. A guard may contain a wall-clock
        // lease expiry, and building a large replay value can outlive the tenure
        // even though no competing writer can modify the record mid-transaction.
        if (transactional_guard) |guard| if (guard.validate_at_commit) |validate|
            try validate(guard.ptr, self.alloc, &txn);
        try batch.commit();
        return promoted_bytes;
    }

    /// Atomically moves existing values to canonical keys while committing the
    /// accompanying mutations and replay record. Promotion values remain
    /// borrowed from the write transaction, so document-sized generations do
    /// not require a second heap copy.
    pub fn putBatchWithPromotionsAndReplay(
        self: *DocStore,
        io: ?std.Io,
        writes: []const KVPair,
        deletes: []const []const u8,
        promotions: []const KeyPromotion,
        replay: ?ReplayAppend,
    ) !usize {
        return try self.putBatchWithPromotionsReplayAndBuiltWrite(io, writes, deletes, promotions, replay, null, null);
    }

    pub fn putBatchWithPromotionsReplayAndBuiltWrite(
        self: *DocStore,
        io: ?std.Io,
        writes: []const KVPair,
        deletes: []const []const u8,
        promotions: []const KeyPromotion,
        replay: ?ReplayAppend,
        transactional_write: ?TransactionalWriteBuilder,
        transactional_guard: ?TransactionalGuard,
    ) !usize {
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            const promoted_bytes = self.putBatchWithPromotionsAndReplayOnce(
                writes,
                deletes,
                promotions,
                replay,
                transactional_write,
                transactional_guard,
            ) catch |err| switch (err) {
                error.WriterLocked => {
                    if (attempt >= writer_locked_retry_count) return err;
                    backoffWriterLockRetry(io);
                    continue;
                },
                else => return err,
            };
            if (replay) |entry| {
                self.markReplayIndexAvailable();
                self.observeCommittedReplaySequence(entry.sequence);
            }
            return promoted_bytes;
        }
    }

    /// Update the in-memory replay watermark after a replay entry was committed
    /// by an external atomic batch (for example transaction intent resolution).
    pub fn observeExternalReplayCommit(self: *DocStore, sequence: u64) void {
        self.markReplayIndexAvailable();
        self.observeCommittedReplaySequence(sequence);
    }

    pub fn lastReplaySequence(self: *DocStore, fallback_last: u64) u64 {
        const next = self.ensureCommittedReplayNextCached(fallback_last + 1);
        return if (next <= 1) 0 else next - 1;
    }

    pub fn lastReplaySequenceFromTxn(_: *DocStore, txn: *Txn, fallback_last: u64) !u64 {
        const raw = txn.get(internal_keys.replay_meta_next_sequence_key[0..]) catch |err| switch (err) {
            error.NotFound => return fallback_last,
            else => return err,
        };
        if (raw.len != 8) return error.CorruptReplayMetadata;
        const next = std.mem.readInt(u64, raw[0..8], .little);
        return if (next <= 1) 0 else next - 1;
    }

    pub fn latestReplaySequenceForHint(self: *DocStore, hint: change_journal_mod.TargetHint, fallback_last: u64) !u64 {
        return try self.latestReplaySequenceForOrdinal(replayHintOrdinal(hint), fallback_last);
    }

    pub fn latestReplaySequenceForOrdinal(self: *DocStore, kind_ordinal: u8, fallback_last: u64) !u64 {
        if (!(try self.hasReplayEntries())) return fallback_last;

        var txn = try self.beginProbeTxn();
        defer txn.abort();

        const key = internal_keys.replayLatestSequenceKey(kind_ordinal);
        const raw = txn.get(key[0..]) catch |err| switch (err) {
            error.NotFound => return fallback_last,
            else => return err,
        };
        const latest = decodeReplaySequence(raw) orelse return error.CorruptReplayMetadata;
        return if (latest > fallback_last) latest else fallback_last;
    }

    pub fn nextReplaySequence(self: *DocStore, fallback_next: u64) u64 {
        return self.ensureReplayNextSequenceCached(fallback_next);
    }

    pub fn reserveNextReplaySequence(self: *DocStore, fallback_next: u64) u64 {
        while (true) {
            if (self.next_replay_sequence_cached.load(.acquire) != 0) {
                return self.next_replay_sequence_cached.fetchAdd(1, .acq_rel);
            }
            _ = self.ensureReplayNextSequenceCached(fallback_next);
        }
    }

    pub fn appendReplayOpaque(self: *DocStore, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var batch = try self.beginWriteBatch();
        errdefer batch.abort();
        try batch.setReplayOpaque(sequence, payload);
        try batch.commit();
        self.markReplayIndexAvailable();
        self.observeCommittedReplaySequence(sequence);
    }

    pub fn iterateReplayFrom(self: *DocStore, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
        return try self.iterateReplayEntriesFromOrdinal(alloc, from_sequence, internal_keys.replay_all_kind);
    }

    pub fn hasReplayEntries(self: *DocStore) !bool {
        switch (self.replay_index_state.load(.monotonic)) {
            replay_index_available => return true,
            replay_index_missing => return false,
            else => {},
        }

        var txn = try self.beginProbeTxn();
        defer txn.abort();
        _ = txn.get(internal_keys.replay_meta_init_key[0..]) catch |err| switch (err) {
            error.NotFound => {
                self.replay_index_state.store(replay_index_missing, .monotonic);
                return false;
            },
            else => return err,
        };
        self.markReplayIndexAvailable();
        return true;
    }

    pub fn ensureReplayIndexInitialized(self: *DocStore) !void {
        if (try self.hasReplayEntries()) return;

        var batch = try self.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(internal_keys.replay_meta_init_key[0..], "");
        const next_raw = encodeReplayNextSequence(1);
        try batch.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
        try batch.commit();
        self.markReplayIndexAvailable();
        _ = self.next_replay_sequence_cached.cmpxchgStrong(0, 1, .acq_rel, .acquire);
        _ = self.committed_replay_next_cached.cmpxchgStrong(0, 1, .acq_rel, .acquire);
    }

    pub fn ensureReplayNextSequenceAtLeast(self: *DocStore, next_sequence: u64) !void {
        const desired_next = @max(next_sequence, @as(u64, 1));
        if (self.ensureCommittedReplayNextCached(1) >= desired_next) return;

        var next_raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &next_raw, desired_next, .little);

        var batch = try self.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(internal_keys.replay_meta_init_key[0..], "");
        try batch.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
        try batch.commit();

        self.markReplayIndexAvailable();
        self.observeCommittedReplaySequence(desired_next - 1);
    }

    fn markReplayIndexAvailable(self: *DocStore) void {
        self.replay_index_state.store(replay_index_available, .monotonic);
    }

    fn loadReplayNextSequenceFromStore(self: *DocStore, fallback_next: u64) u64 {
        var txn = self.beginProbeTxn() catch return fallback_next;
        defer txn.abort();
        const raw = txn.get(internal_keys.replay_meta_next_sequence_key[0..]) catch return fallback_next;
        if (raw.len != 8) return fallback_next;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    fn ensureReplayNextSequenceCached(self: *DocStore, fallback_next: u64) u64 {
        const cached = self.next_replay_sequence_cached.load(.acquire);
        if (cached != 0) return cached;

        const loaded = self.loadReplayNextSequenceFromStore(fallback_next);
        if (self.next_replay_sequence_cached.cmpxchgStrong(0, loaded, .acq_rel, .acquire)) |existing| {
            return existing;
        }
        return loaded;
    }

    fn observeCommittedReplaySequence(self: *DocStore, sequence: u64) void {
        const desired_next = sequence + 1;
        while (true) {
            const current = self.committed_replay_next_cached.load(.acquire);
            if (current >= desired_next) break;
            if (self.committed_replay_next_cached.cmpxchgWeak(current, desired_next, .acq_rel, .acquire) == null) break;
        }
        while (true) {
            const current = self.next_replay_sequence_cached.load(.acquire);
            if (current >= desired_next) return;
            if (self.next_replay_sequence_cached.cmpxchgWeak(current, desired_next, .acq_rel, .acquire) == null) return;
        }
    }

    fn ensureCommittedReplayNextCached(self: *DocStore, fallback_next: u64) u64 {
        const cached = self.committed_replay_next_cached.load(.acquire);
        if (cached != 0) return cached;
        // A transient probe/read failure is not a durable empty cut. Leave
        // the cache unknown so the next observation retries recovery.
        var txn = self.beginProbeTxn() catch return fallback_next;
        defer txn.abort();
        const raw = txn.get(internal_keys.replay_meta_next_sequence_key[0..]) catch |err| switch (err) {
            error.NotFound => return fallback_next,
            else => return fallback_next,
        };
        if (raw.len != 8) return fallback_next;
        const loaded = std.mem.readInt(u64, raw[0..8], .little);
        return self.committed_replay_next_cached.cmpxchgStrong(0, loaded, .acq_rel, .acquire) orelse loaded;
    }

    fn iterateReplayEntriesFromOrdinal(
        self: *DocStore,
        alloc: Allocator,
        from_sequence: u64,
        kind_ordinal: u8,
    ) ![]backend_types.ReplayEntry {
        if (!(try self.hasReplayEntries())) return error.ReplayIndexUnavailable;

        var entries = std.ArrayListUnmanaged(backend_types.ReplayEntry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        }

        const Context = struct {
            alloc: Allocator,
            entries: *std.ArrayListUnmanaged(backend_types.ReplayEntry),

            fn handle(self_ctx: *@This(), sequence: u64, payload: []const u8) !void {
                try self_ctx.entries.append(self_ctx.alloc, .{
                    .sequence = sequence,
                    .payload = try self_ctx.alloc.dupe(u8, payload),
                });
            }
        };

        var ctx = Context{
            .alloc = alloc,
            .entries = &entries,
        };
        try self.forEachReplayEntryFromOrdinal(from_sequence, kind_ordinal, &ctx, Context.handle);
        return try entries.toOwnedSlice(alloc);
    }

    fn forEachReplayEntryFromOrdinal(
        self: *DocStore,
        from_sequence: u64,
        kind_ordinal: u8,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        if (!(try self.hasReplayEntries())) return error.ReplayIndexUnavailable;
        _ = try self.forEachReplayLaneFrom(kind_ordinal, from_sequence, 0, ctx, callback);
    }

    pub fn forEachReplayLaneFrom(
        self: *DocStore,
        kind_ordinal: u8,
        from_sequence: u64,
        max_entries: usize,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !ReplayIterationStats {
        if (!(try self.hasReplayEntries())) return error.ReplayIndexUnavailable;

        const lane_stats = try self.runtime_store.forEachReplayLaneFrom(kind_ordinal, from_sequence, max_entries, ctx, callback);
        return .{
            .scanned_entries = lane_stats.scanned_entries,
            .matched_entries = lane_stats.matched_entries,
            .last_sequence = lane_stats.last_sequence,
            .scan_batches = lane_stats.scan_batches,
            .fallback_used = lane_stats.fallback_used,
        };
    }

    pub fn iterateReplayEntriesFromHint(
        self: *DocStore,
        alloc: Allocator,
        from_sequence: u64,
        hint: change_journal_mod.TargetHint,
    ) ![]backend_types.ReplayEntry {
        return try self.iterateReplayEntriesFromOrdinal(alloc, from_sequence, replayHintOrdinal(hint));
    }

    pub fn forEachReplayEntryFromHint(
        self: *DocStore,
        from_sequence: u64,
        hint: change_journal_mod.TargetHint,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        return try self.forEachReplayEntryFromOrdinal(from_sequence, replayHintOrdinal(hint), ctx, callback);
    }

    pub fn forEachReplayFrom(
        self: *DocStore,
        from_sequence: u64,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        return try self.forEachReplayEntryFromOrdinal(from_sequence, internal_keys.replay_all_kind, ctx, callback);
    }

    pub fn forEachReplayFromMatchingHintMask(
        self: *DocStore,
        from_sequence: u64,
        required_hint_mask: u8,
        callback_ctx: *anyopaque,
        callback: backend_erased.Store.ReplayCallback,
    ) !void {
        var stats = ReplayIterationStats{};
        return try self.forEachReplayFromMatchingHintMaskWithStats(from_sequence, required_hint_mask, callback_ctx, callback, &stats);
    }

    pub fn forEachReplayFromMatchingHintMaskWithStats(
        self: *DocStore,
        from_sequence: u64,
        required_hint_mask: u8,
        callback_ctx: *anyopaque,
        callback: backend_erased.Store.ReplayCallback,
        stats: *ReplayIterationStats,
    ) !void {
        const Context = struct {
            callback_ctx: *anyopaque,
            callback: backend_erased.Store.ReplayCallback,

            fn pass(self_ctx: *@This(), sequence: u64, payload: []const u8) !void {
                try self_ctx.callback(self_ctx.callback_ctx, sequence, payload);
            }
        };

        var ctx = Context{
            .callback_ctx = callback_ctx,
            .callback = callback,
        };
        const lane_stats = if (required_hint_mask == 0)
            try self.forEachReplayLaneFrom(internal_keys.replay_all_kind, from_sequence, 0, &ctx, Context.pass)
        else if (replayHintFromSingleMask(required_hint_mask)) |hint|
            try self.forEachReplayLaneFrom(replayHintOrdinal(hint), from_sequence, 0, &ctx, Context.pass)
        else
            return error.Unsupported;
        stats.scanned_entries += lane_stats.scanned_entries;
        stats.matched_entries += lane_stats.matched_entries;
        stats.last_sequence = lane_stats.last_sequence;
        stats.scan_batches += lane_stats.scan_batches;
        stats.fallback_used = lane_stats.fallback_used;
    }

    pub fn forEachReplayFromMatchingHint(
        self: *DocStore,
        from_sequence: u64,
        hint: change_journal_mod.TargetHint,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        return try self.forEachReplayEntryFromHint(from_sequence, hint, ctx, callback);
    }

    pub fn truncateReplayUpTo(self: *DocStore, alloc: Allocator, up_to_sequence: u64) !void {
        try self.truncateReplayEntries(alloc, up_to_sequence);
    }

    fn truncateReplayEntries(self: *DocStore, alloc: Allocator, up_to_sequence: u64) !void {
        if (up_to_sequence == 0) return;
        if (!(try self.hasReplayEntries())) return;

        var deletes = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(key);
            deletes.deinit(alloc);
        }
        try deletes.ensureTotalCapacityPrecise(alloc, replay_truncate_batch_keys);

        // Replay lanes are append-only. Capturing each lane independently is
        // sufficient below an acknowledged retirement watermark, and avoids
        // cloning/sorting unrelated document and artifact keys after every
        // enrichment completion.
        for (0..replay_hints.len + 1) |lane| {
            const ordinal = if (lane < replay_hints.len) replayHintOrdinal(replay_hints[lane]) else internal_keys.replay_all_kind;
            var from_sequence: u64 = 0;
            while (true) {
                var last_sequence = from_sequence;
                {
                    var txn = try self.beginReplayLaneScanTxn(ordinal, from_sequence);
                    defer txn.abort();
                    var cur = try txn.openCursor();
                    defer cur.close();
                    const lower = internal_keys.replayRangeLower(ordinal, from_sequence);
                    const upper = internal_keys.replayRangeUpper(ordinal);
                    cur.setUpperBound(&upper);
                    var entry = try cur.seekAtOrAfter(&lower);
                    while (entry) |kv| : (entry = try cur.next()) {
                        if (std.mem.order(u8, kv.key, &upper) != .lt) break;
                        const sequence = internal_keys.parseReplayEntrySequence(kv.key, ordinal) orelse break;
                        if (sequence > up_to_sequence) break;
                        const key = try alloc.dupe(u8, kv.key);
                        errdefer alloc.free(key);
                        try deletes.append(alloc, key);
                        last_sequence = sequence;
                        if (deletes.items.len == replay_truncate_batch_keys) break;
                    }
                }
                if (deletes.items.len == 0) break;
                // Release the read generation before each bounded commit.
                // Acknowledged rows are never replayed again; interruption
                // leaves idempotent progress for the next maintenance pass.
                var batch = try self.beginWriteBatch();
                errdefer batch.abort();
                for (deletes.items) |key| {
                    batch.delete(key) catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                }
                try batch.commit();
                for (deletes.items) |key| alloc.free(key);
                deletes.clearRetainingCapacity();
                if (last_sequence >= up_to_sequence or last_sequence == std.math.maxInt(u64)) break;
                from_sequence = last_sequence + 1;
            }
        }
    }

    const replay_truncate_batch_keys: usize = 4096;

    /// Scan all keys with the given prefix. Caller owns returned slices.
    pub fn scanPrefix(self: *DocStore, alloc: Allocator, prefix: []const u8) ![]OwnedKVPair {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        return try scanPrefixTxn(alloc, &txn, prefix);
    }

    /// Scan a prefix from an existing point-in-time read transaction.
    pub fn scanPrefixTxn(alloc: Allocator, txn: *Txn, prefix: []const u8) ![]OwnedKVPair {
        var cur = try txn.openCursor();
        defer cur.close();

        var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
        errdefer {
            for (results.items) |item| {
                alloc.free(item.key);
                alloc.free(item.value);
            }
            results.deinit(alloc);
        }

        // Seek to first key >= prefix
        const first = (try cur.seekAtOrAfter(prefix)) orelse return try alloc.dupe(OwnedKVPair, results.items);

        if (std.mem.startsWith(u8, first.key, prefix)) {
            try appendOwnedKVPairCopy(alloc, &results, first.key, first.value);
        } else {
            return try alloc.dupe(OwnedKVPair, results.items);
        }

        var entry = try cur.next();
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            try appendOwnedKVPairCopy(alloc, &results, kv.key, kv.value);
        }

        const owned = try alloc.dupe(OwnedKVPair, results.items);
        results.deinit(alloc);
        return owned;
    }

    /// Scan up to `limit` keys with the given prefix after `after_key`.
    /// Caller owns returned slices.
    pub fn scanPrefixPage(
        self: *DocStore,
        alloc: Allocator,
        prefix: []const u8,
        after_key: ?[]const u8,
        limit: usize,
    ) ![]OwnedKVPair {
        if (limit == 0) return try alloc.dupe(OwnedKVPair, &.{});

        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
        errdefer {
            for (results.items) |item| {
                alloc.free(item.key);
                alloc.free(item.value);
            }
            results.deinit(alloc);
        }

        const bounded_after_key = if (after_key) |key| if (std.mem.startsWith(u8, key, prefix)) key else null else null;
        const seek_key = bounded_after_key orelse prefix;
        var entry = (try cur.seekAtOrAfter(seek_key)) orelse return try alloc.dupe(OwnedKVPair, results.items);
        while (true) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            if (bounded_after_key == null or std.mem.order(u8, entry.key, bounded_after_key.?) == .gt) {
                try appendOwnedKVPairCopy(alloc, &results, entry.key, entry.value);
                if (results.items.len >= limit) break;
            }
            entry = (try cur.next()) orelse break;
        }

        const owned = try alloc.dupe(OwnedKVPair, results.items);
        results.deinit(alloc);
        return owned;
    }

    /// Scan only keys with the given prefix after `after_key`. Unlike
    /// `scanPrefixPage`, this never copies values into the caller allocator;
    /// maintenance cursors can therefore discover a very large value without
    /// transiently materializing it before their own admission checks run.
    pub fn scanPrefixKeysPage(
        self: *DocStore,
        alloc: Allocator,
        prefix: []const u8,
        after_key: ?[]const u8,
        limit: usize,
    ) ![][]u8 {
        if (limit == 0) return try alloc.dupe([]u8, &.{});

        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openPhysicalKeyCursorAdapter();
        defer cur.close();

        var results = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (results.items) |key| alloc.free(key);
            results.deinit(alloc);
        }

        const bounded_after_key = if (after_key) |key| if (std.mem.startsWith(u8, key, prefix)) key else null else null;
        const seek_key = bounded_after_key orelse prefix;
        var entry = (try cur.seekAtOrAfter(seek_key)) orelse return try results.toOwnedSlice(alloc);
        while (true) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            if (bounded_after_key == null or std.mem.order(u8, entry.key, bounded_after_key.?) == .gt) {
                try results.append(alloc, try alloc.dupe(u8, entry.key));
                if (results.items.len >= limit) break;
            }
            entry = (try cur.next()) orelse break;
        }
        return try results.toOwnedSlice(alloc);
    }

    /// Scan keys in [lower, upper). Caller owns returned slices.
    pub fn scanRange(self: *DocStore, alloc: Allocator, lower: []const u8, upper: []const u8) ![]OwnedKVPair {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        return try scanRangeTxn(alloc, &txn, lower, upper);
    }

    /// Scan a range from an existing point-in-time read transaction.
    pub fn scanRangeTxn(alloc: Allocator, txn: *Txn, lower: []const u8, upper: []const u8) ![]OwnedKVPair {
        var cur = try txn.openCursor();
        defer cur.close();
        cur.setUpperBound(if (upper.len > 0) upper else null);

        var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
        errdefer {
            for (results.items) |item| {
                alloc.free(item.key);
                alloc.free(item.value);
            }
            results.deinit(alloc);
        }

        // Seek to first key >= lower, or first key in the DB when lower is empty.
        const first = if (lower.len == 0)
            (try cur.first()) orelse return try alloc.dupe(OwnedKVPair, results.items)
        else
            (try cur.seekAtOrAfter(lower)) orelse return try alloc.dupe(OwnedKVPair, results.items);

        if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) {
            return try alloc.dupe(OwnedKVPair, results.items);
        }

        try appendOwnedKVPairCopy(alloc, &results, first.key, first.value);

        var entry = try cur.next();
        while (entry) |kv| : (entry = try cur.next()) {
            if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
            try appendOwnedKVPairCopy(alloc, &results, kv.key, kv.value);
        }

        const owned = try alloc.dupe(OwnedKVPair, results.items);
        results.deinit(alloc);
        return owned;
    }

    /// Scan only keys in [lower, upper). This avoids copying document values
    /// when callers need an atomic delete set for generation replacement.
    pub fn scanRangeKeys(self: *DocStore, alloc: Allocator, lower: []const u8, upper: []const u8) ![][]u8 {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openPhysicalKeyCursorAdapter();
        defer cur.close();
        cur.setUpperBound(if (upper.len > 0) upper else null);

        var keys = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (keys.items) |key| alloc.free(key);
            keys.deinit(alloc);
        }
        var entry = if (lower.len == 0) try cur.first() else try cur.seekAtOrAfter(lower);
        while (entry) |kv| : (entry = try cur.next()) {
            if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
            const key = try alloc.dupe(u8, kv.key);
            errdefer alloc.free(key);
            try keys.append(alloc, key);
        }
        return try keys.toOwnedSlice(alloc);
    }

    pub fn findMedianKey(self: *DocStore, alloc: Allocator, lower: []const u8, upper: []const u8, options: ScanOptions) ![]u8 {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        const visible_count = try countVisibleRange(&txn, lower, upper, options);
        if (visible_count == 0) return error.NotFound;

        return try copyVisibleKeyAtIndex(&txn, alloc, lower, upper, options, visible_count / 2);
    }

    // ====================================================================
    // Streaming scan — constant memory, callback-based
    // ====================================================================

    pub const ScanOptions = struct {
        physical_payloads: bool = false,
        /// Return true to skip this key (callback not invoked).
        skip_fn: ?*const fn (key: []const u8) bool = null,
        reverse: bool = false,
        /// When set, scan starts at the first key strictly greater than lower.
        lower_exclusive: bool = false,
    };

    pub const ScanAction = enum { @"continue", stop };

    pub const ScanWithContextCallback = *const fn (
        ctx: ?*anyopaque,
        key: []const u8,
        value: []const u8,
    ) anyerror!ScanAction;

    /// Streaming scan over [lower, upper). Constant memory — callback sees
    /// borrowed storage slices directly (valid only for duration of call).
    /// If upper is empty, scans to end of database.
    pub fn scan(
        self: *DocStore,
        lower: []const u8,
        upper: []const u8,
        options: ScanOptions,
        callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,
    ) !void {
        const Adapter = struct {
            callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,

            fn run(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!ScanAction {
                const adapter: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                return try adapter.callback(key, value);
            }
        };
        var adapter = Adapter{ .callback = callback };
        return try self.scanWithContext(lower, upper, options, &adapter, Adapter.run);
    }

    pub fn scanWithContext(
        self: *DocStore,
        lower: []const u8,
        upper: []const u8,
        options: ScanOptions,
        ctx: ?*anyopaque,
        callback: ScanWithContextCallback,
    ) !void {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        try self.scanReadTxnWithContext(&txn, lower, upper, options, ctx, callback);
    }

    /// Privileged scan used only to roll back a publication while ordinary
    /// readers remain fenced. Recovery-required still blocks this handle.
    pub fn scanPortableImportRollbackWithContext(
        self: *DocStore,
        lower: []const u8,
        upper: []const u8,
        options: ScanOptions,
        ctx: ?*anyopaque,
        callback: ScanWithContextCallback,
    ) !void {
        try self.ensurePortableImportOperational();
        var txn = try self.beginReadTxnUnchecked();
        defer txn.abort();
        try self.scanReadTxnWithContext(&txn, lower, upper, options, ctx, callback);
    }

    /// Row-only owner traversal. Never advance through an owner's artifact
    /// tail: seek directly to the next encoded document prefix. Checkpoints
    /// run once per owner, including artifact-only owners, so cancellation and
    /// maintenance budgets do not depend on finding another live row.
    pub fn scanRelationalRowsReadTxnWithContext(
        self: *DocStore,
        txn: *Txn,
        lower: []const u8,
        upper: []const u8,
        ctx: ?*anyopaque,
        checkpoint: *const fn (?*anyopaque, []const u8) anyerror!ScanAction,
        callback: ScanWithContextCallback,
    ) !void {
        return self.scanRowKindReadTxnWithContext(txn, lower, upper, internal_keys.relational_row_kind, ctx, checkpoint, callback);
    }

    pub fn scanDocumentRowsReadTxnWithContext(
        self: *DocStore,
        txn: *Txn,
        lower: []const u8,
        upper: []const u8,
        ctx: ?*anyopaque,
        checkpoint: *const fn (?*anyopaque, []const u8) anyerror!ScanAction,
        callback: ScanWithContextCallback,
    ) !void {
        return self.scanRowKindReadTxnWithContext(txn, lower, upper, internal_keys.primary_kind, ctx, checkpoint, callback);
    }

    fn scanRowKindReadTxnWithContext(
        self: *DocStore,
        txn: *Txn,
        lower: []const u8,
        upper: []const u8,
        row_kind: u8,
        ctx: ?*anyopaque,
        checkpoint: *const fn (?*anyopaque, []const u8) anyerror!ScanAction,
        callback: ScanWithContextCallback,
    ) !void {
        var cursor = try txn.openCursor();
        defer cursor.close();
        const end = if (upper.len != 0) upper else &[_]u8{internal_keys.user_namespace + 1};
        cursor.setUpperBound(end);
        const owner_start = &[_]u8{internal_keys.user_namespace};
        const start = if (std.mem.order(u8, lower, owner_start) == .lt) owner_start else lower;
        if (std.mem.order(u8, start, end) != .lt) return;
        var candidate = std.ArrayListUnmanaged(u8).empty;
        defer candidate.deinit(self.alloc);
        var entry = try cursor.seekAtOrAfter(start);
        while (entry) |item| {
            if (std.mem.order(u8, item.key, end) != .lt or !internal_keys.isInternalUserKey(item.key)) return;
            const term = internal_keys.findComponentTerminator(item.key, 1) orelse return error.InvalidInternalUserKey;
            candidate.clearRetainingCapacity();
            try candidate.appendSlice(self.alloc, item.key[0 .. term + 2]);
            try candidate.append(self.alloc, row_kind);
            if (try checkpoint(ctx, candidate.items) == .stop) return;
            var current = item;
            if (std.mem.order(u8, current.key, candidate.items) == .lt) {
                current = (try cursor.seekAtOrAfter(candidate.items)) orelse return;
                if (!std.mem.startsWith(u8, current.key, candidate.items[0 .. term + 2])) {
                    entry = current;
                    continue;
                }
            }
            if (std.mem.eql(u8, current.key, candidate.items)) {
                if (try callback(ctx, current.key, current.value) == .stop) return;
            }
            // The final byte of the escaped owner's terminator is zero.
            // Its successor skips this owner, not documents extending its ID.
            candidate.items.len = term + 2;
            candidate.items[term + 1] = 1;
            if (std.mem.order(u8, candidate.items, end) != .lt) return;
            entry = try cursor.seekAtOrAfter(candidate.items);
        }
    }

    /// Copy a bounded page from a caller-owned snapshot. Keeping the transaction
    /// across pages preserves its source sequence during concurrent writes.
    pub fn scanReadTxnPage(
        self: *DocStore,
        alloc: Allocator,
        txn: *Txn,
        lower: []const u8,
        lower_exclusive: bool,
        upper: []const u8,
        max_items: usize,
        max_bytes: usize,
    ) !backend_scan.RangePage {
        if (max_items == 0 or max_bytes == 0) return error.InvalidArgument;
        const Context = struct {
            alloc: Allocator,
            lower: []const u8,
            exclusive: bool,
            max_items: usize,
            max_bytes: usize,
            bytes: usize = 0,
            reached_end: bool = true,
            items: std.ArrayListUnmanaged(backend_scan.OwnedKVPair) = .empty,

            fn visit(raw: ?*anyopaque, key: []const u8, value: []const u8) anyerror!ScanAction {
                const ctx: *@This() = @ptrCast(@alignCast(raw.?));
                if (ctx.exclusive and std.mem.eql(u8, key, ctx.lower)) return .@"continue";
                const bytes = std.math.add(usize, key.len, value.len) catch return error.OutOfMemory;
                if (ctx.items.items.len > 0 and
                    (ctx.items.items.len >= ctx.max_items or ctx.bytes > ctx.max_bytes -| bytes))
                {
                    ctx.reached_end = false;
                    return .stop;
                }
                const owned_key = try ctx.alloc.dupe(u8, key);
                errdefer ctx.alloc.free(owned_key);
                const owned_value = try ctx.alloc.dupe(u8, value);
                errdefer ctx.alloc.free(owned_value);
                try ctx.items.append(ctx.alloc, .{ .key = owned_key, .value = owned_value });
                ctx.bytes +|= bytes;
                return .@"continue";
            }
        };
        var ctx = Context{ .alloc = alloc, .lower = lower, .exclusive = lower_exclusive, .max_items = max_items, .max_bytes = max_bytes };
        errdefer {
            for (ctx.items.items) |item| {
                alloc.free(item.key);
                alloc.free(item.value);
            }
            ctx.items.deinit(alloc);
        }
        try self.scanReadTxnWithContext(txn, lower, upper, .{}, &ctx, Context.visit);
        return .{ .items = try ctx.items.toOwnedSlice(alloc), .reached_end = ctx.reached_end };
    }

    pub fn scanReadTxnWithContext(
        self: *DocStore,
        txn: *Txn,
        lower: []const u8,
        upper: []const u8,
        options: ScanOptions,
        ctx: ?*anyopaque,
        callback: ScanWithContextCallback,
    ) !void {
        _ = self;
        var cur = if (options.physical_payloads) try txn.openDocumentCursorAdapter(true) else try txn.openCursor();
        defer cur.close();
        if (!options.reverse) {
            cur.setUpperBound(if (upper.len > 0) upper else null);

            // Seek to first key >= lower (use .first when lower is empty)
            const first = if (lower.len == 0)
                (try cur.first()) orelse return
            else
                (try cur.seekAtOrAfter(lower)) orelse return;

            // Check upper bound
            if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) return;

            // Process first entry
            if (!options.lower_exclusive or lower.len == 0 or !std.mem.eql(u8, first.key, lower)) {
                if (options.skip_fn == null or !options.skip_fn.?(first.key)) {
                    const action = try callback(ctx, first.key, first.value);
                    if (action == .stop) return;
                }
            }

            // Iterate remaining
            var entry = try cur.next();
            while (entry) |kv| : (entry = try cur.next()) {
                if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
                if (options.skip_fn) |skip| {
                    if (skip(kv.key)) continue;
                }
                const action = try callback(ctx, kv.key, kv.value);
                if (action == .stop) return;
            }
            return;
        }

        var entry = if (upper.len == 0)
            try cur.last()
        else
            try cur.seekAtOrBefore(upper);
        while (entry) |kv| {
            if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) {
                entry = try cur.prev();
                continue;
            }
            if (lower.len > 0 and std.mem.order(u8, kv.key, lower) == .lt) break;
            if (options.skip_fn == null or !options.skip_fn.?(kv.key)) {
                const action = try callback(ctx, kv.key, kv.value);
                if (action == .stop) return;
            }
            entry = try cur.prev();
        }
    }

    /// Free results from scanPrefix or scanRange.
    pub fn freeResults(alloc: Allocator, results: []OwnedKVPair) void {
        for (results) |item| {
            alloc.free(item.key);
            alloc.free(item.value);
        }
        alloc.free(results);
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
            if (@typeInfo(ptr.child) == .@"struct" and @hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        .@"struct" => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {},
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn countVisibleRange(
    txn: *DocStore.Txn,
    lower: []const u8,
    upper: []const u8,
    options: DocStore.ScanOptions,
) !usize {
    var cur = try txn.openCursor();
    defer cur.close();
    cur.setKeysOnly();
    cur.setUpperBound(if (upper.len > 0) upper else null);

    const first = if (lower.len == 0)
        (try cur.first()) orelse return 0
    else
        (try cur.seekAtOrAfter(lower)) orelse return 0;

    if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) return 0;

    var count: usize = 0;
    var entry = first;
    while (true) {
        if (options.skip_fn) |skip| {
            if (!skip(entry.key)) count += 1;
        } else {
            count += 1;
        }

        entry = (try cur.next()) orelse break;
        if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break;
    }

    return count;
}

fn copyVisibleKeyAtIndex(
    txn: *DocStore.Txn,
    alloc: Allocator,
    lower: []const u8,
    upper: []const u8,
    options: DocStore.ScanOptions,
    target_index: usize,
) ![]u8 {
    var cur = try txn.openCursor();
    defer cur.close();
    cur.setKeysOnly();
    cur.setUpperBound(if (upper.len > 0) upper else null);

    var entry = if (lower.len == 0)
        (try cur.first()) orelse return error.NotFound
    else
        (try cur.seekAtOrAfter(lower)) orelse return error.NotFound;

    if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) return error.NotFound;

    var count: usize = 0;
    while (true) {
        if (options.skip_fn) |skip| {
            if (!skip(entry.key)) {
                if (count == target_index) return try alloc.dupe(u8, entry.key);
                count += 1;
            }
        } else {
            if (count == target_index) return try alloc.dupe(u8, entry.key);
            count += 1;
        }

        entry = (try cur.next()) orelse break;
        if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break;
    }

    return error.NotFound;
}

// ============================================================================
// KeyEncoder — static key construction helpers
// ============================================================================

pub const KeyEncoder = struct {
    /// Build edge key: <source>:i:<indexName>:out:<edgeType>:<target>:o
    pub fn makeEdgeKey(buf: []u8, source: []const u8, index_name: []const u8, edge_type: []const u8, target: []const u8) []const u8 {
        const result = std.fmt.bufPrint(buf, "{s}:i:{s}:out:{s}:{s}:o", .{ source, index_name, edge_type, target }) catch unreachable;
        return result;
    }

    /// Build reverse edge key: <target>:i:<indexName>:in:<edgeType>:<source>:i
    pub fn makeReverseEdgeKey(buf: []u8, target: []const u8, index_name: []const u8, edge_type: []const u8, source: []const u8) []const u8 {
        const result = std.fmt.bufPrint(buf, "{s}:i:{s}:in:{s}:{s}:i", .{ target, index_name, edge_type, source }) catch unreachable;
        return result;
    }

    /// Build edge prefix for scanning: <key>:i:<indexName>:out:<edgeType>:
    /// If edge_type is empty, prefix is: <key>:i:<indexName>:out:
    pub fn makeEdgePrefix(buf: []u8, key: []const u8, index_name: []const u8, edge_type: []const u8) []const u8 {
        if (edge_type.len > 0) {
            return std.fmt.bufPrint(buf, "{s}:i:{s}:out:{s}:", .{ key, index_name, edge_type }) catch unreachable;
        }
        return std.fmt.bufPrint(buf, "{s}:i:{s}:out:", .{ key, index_name }) catch unreachable;
    }

    /// Build reverse edge prefix: <key>:i:<indexName>:in:<edgeType>:
    pub fn makeReverseEdgePrefix(buf: []u8, key: []const u8, index_name: []const u8, edge_type: []const u8) []const u8 {
        if (edge_type.len > 0) {
            return std.fmt.bufPrint(buf, "{s}:i:{s}:in:{s}:", .{ key, index_name, edge_type }) catch unreachable;
        }
        return std.fmt.bufPrint(buf, "{s}:i:{s}:in:", .{ key, index_name }) catch unreachable;
    }

    /// Build embedding key: <doc>:i:<indexName>:e
    pub fn makeEmbeddingKey(buf: []u8, doc_key: []const u8, index_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:i:{s}:e", .{ doc_key, index_name }) catch unreachable;
    }

    /// Build summary key: <doc>:i:<indexName>:s
    pub fn makeSummaryKey(buf: []u8, doc_key: []const u8, index_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:i:{s}:s", .{ doc_key, index_name }) catch unreachable;
    }

    /// Build chunk key: <doc>:i:<indexName>:<chunkID>:c
    pub fn makeChunkKey(buf: []u8, doc_key: []const u8, index_name: []const u8, chunk_id: u32) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:i:{s}:{d}:c", .{ doc_key, index_name, chunk_id }) catch unreachable;
    }

    /// Build enrichment prefix: <doc>:e:<type>:<name>:
    pub fn makeEnrichmentPrefix(buf: []u8, doc_key: []const u8, enrichment_type: []const u8, enrichment_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:{s}:{s}:", .{ doc_key, enrichment_type, enrichment_name }) catch unreachable;
    }

    /// Build root enrichment prefix: <doc>:e:
    pub fn makeEnrichmentRootPrefix(buf: []u8, doc_key: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:", .{doc_key}) catch unreachable;
    }

    /// Build enrichment type prefix: <doc>:e:<type>:
    pub fn makeEnrichmentTypePrefix(buf: []u8, doc_key: []const u8, enrichment_type: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:{s}:", .{ doc_key, enrichment_type }) catch unreachable;
    }

    /// Build enrichment chunk key: <doc>:e:chunk:<name>:<chunkID>
    pub fn makeEnrichmentChunkKey(buf: []u8, doc_key: []const u8, enrichment_name: []const u8, chunk_id: u32) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:chunk:{s}:{d}", .{ doc_key, enrichment_name, chunk_id }) catch unreachable;
    }

    /// Build enrichment embedding key: <base>:e:embedding:<name>
    pub fn makeEnrichmentEmbeddingKey(buf: []u8, base_key: []const u8, enrichment_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:embedding:{s}", .{ base_key, enrichment_name }) catch unreachable;
    }

    /// Range start sentinel: <key>:\x00
    pub fn keyRangeStart(buf: []u8, key: []const u8) []const u8 {
        @memcpy(buf[0..key.len], key);
        buf[key.len] = ':';
        buf[key.len + 1] = 0x00;
        return buf[0 .. key.len + 2];
    }

    /// Range end sentinel: <key>:\xFF
    pub fn keyRangeEnd(buf: []u8, key: []const u8) []const u8 {
        @memcpy(buf[0..key.len], key);
        buf[key.len] = ':';
        buf[key.len + 1] = 0xFF;
        return buf[0 .. key.len + 2];
    }

    /// Check if a key is an edge key (contains ":i:" and ends with ":o").
    pub fn isEdgeKey(key: []const u8) bool {
        if (key.len < 6) return false; // minimum: "x:i:y:o"
        if (key[key.len - 2] != ':' or key[key.len - 1] != 'o') return false;
        return std.mem.indexOf(u8, key, ":i:") != null;
    }

    /// Parsed edge key components.
    pub const ParsedEdgeKey = struct {
        source: []const u8,
        index_name: []const u8,
        edge_type: []const u8,
        target: []const u8,
    };

    /// Parse an outgoing edge key: <source>:i:<indexName>:out:<edgeType>:<target>:o
    /// Also handles reverse edge keys: <target>:i:<indexName>:in:<edgeType>:<source>:i
    pub fn parseEdgeKey(key: []const u8) ?ParsedEdgeKey {
        // Find ":i:" marker
        const idx_marker = std.mem.indexOf(u8, key, ":i:") orelse return null;
        const before = key[0..idx_marker];
        const after_marker = key[idx_marker + 3 ..]; // <indexName>:out/in:<edgeType>:<target>:o/i

        // Try outgoing: <indexName>:out:<edgeType>:<target>:o
        if (std.mem.indexOf(u8, after_marker, ":out:")) |out_pos| {
            const index_name = after_marker[0..out_pos];
            const after_out = after_marker[out_pos + 5 ..]; // <edgeType>:<target>:o
            // Must end with ":o"
            if (after_out.len < 2) return null;
            if (after_out[after_out.len - 2] != ':' or after_out[after_out.len - 1] != 'o') return null;
            const rest = after_out[0 .. after_out.len - 2]; // <edgeType>:<target>
            const type_end = std.mem.indexOf(u8, rest, ":") orelse return null;
            return .{
                .source = before,
                .index_name = index_name,
                .edge_type = rest[0..type_end],
                .target = rest[type_end + 1 ..],
            };
        }

        // Try incoming: <indexName>:in:<edgeType>:<source>:i
        if (std.mem.indexOf(u8, after_marker, ":in:")) |in_pos| {
            const index_name = after_marker[0..in_pos];
            const after_in = after_marker[in_pos + 4 ..]; // <edgeType>:<source>:i
            // Must end with ":i"
            if (after_in.len < 2) return null;
            if (after_in[after_in.len - 2] != ':' or after_in[after_in.len - 1] != 'i') return null;
            const rest = after_in[0 .. after_in.len - 2]; // <edgeType>:<source>
            const type_end = std.mem.indexOf(u8, rest, ":") orelse return null;
            return .{
                .source = rest[type_end + 1 ..],
                .index_name = index_name,
                .edge_type = rest[0..type_end],
                .target = before,
            };
        }

        return null;
    }
};

// ============================================================================
// Tests
// ============================================================================

var tmp_path_nonce: u64 = 0;

fn tmpPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-docstore-test-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &tmp_path_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    fs_paths.createDirPathPortable(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test {
    _ = @import("document_input.zig");
    _ = @import("index_source_claim_test.zig");
}

test "docstore put/get/delete" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("doc1", "hello");
    try store.put("doc2", "world");

    const val1 = try store.get(std.testing.allocator, "doc1");
    defer std.testing.allocator.free(val1);
    try std.testing.expectEqualStrings("hello", val1);

    const val2 = try store.get(std.testing.allocator, "doc2");
    defer std.testing.allocator.free(val2);
    try std.testing.expectEqualStrings("world", val2);

    try store.delete("doc1");
    try std.testing.expectError(error.NotFound, store.get(std.testing.allocator, "doc1"));
}

test "docstore retained row effects atomically coalesce and fence bounded source consumers" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.put(&internal_keys.identity_namespace_key, &@as(retained_effects.Namespace, @splat(1)));
    const a = try internal_keys.documentKeyAlloc(alloc, "a");
    defer alloc.free(a);
    const b = try internal_keys.documentKeyAlloc(alloc, "b");
    defer alloc.free(b);
    // Inactive stores cache absence and allocate no capture keys/values.
    try store.put(a, "initial");
    try std.testing.expectEqual(@as(u8, 1), store.retained_effects_cache.load(.acquire));
    const pin: [32]u8 = @splat(1);
    const other: [32]u8 = @splat(2);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(u64, 0), try retained_effects.admit(&txn, @splat(1), 1, pin, retained_effects.default_limit));
        try std.testing.expectEqual(@as(u64, 0), try retained_effects.admit(&txn, @splat(1), 2, other, retained_effects.default_limit));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(a, "aborted");
        try txn.delete(a);
    }
    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(a, "intermediate");
        try batch.put(a, "final");
        try batch.put(b, "deleted in same transaction");
        try batch.delete(b);
        try batch.put("unrelated-index-key", "not a row");
        try batch.commit();
    }
    {
        const raw = try store.get(alloc, &retained_effects.recordKey(1));
        defer alloc.free(raw);
        var reader = try retained_effects.Reader.init(raw, 1);
        const first = (try reader.next()).?;
        try std.testing.expectEqualStrings(a, first.key);
        try std.testing.expectEqualStrings("final", first.value.?);
        const second = (try reader.next()).?;
        try std.testing.expectEqualStrings(b, second.key);
        try std.testing.expect(second.value == null);
        try std.testing.expect(try reader.next() == null);
        raw[raw.len - 1] ^= 1;
        try std.testing.expectError(error.RetainedEffectsCorrupt, retained_effects.Reader.init(raw, 1));
    }
    // A write transaction (including transaction-resolution adapters) shares
    // the same capture as batches; metadata-only writes don't consume sequence.
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(a, "second");
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(u64, 2), (try retained_effects.load(&txn)).?.latest);
        try std.testing.expectError(error.RetainedEffectsCursorMismatch, retained_effects.acknowledge(&txn, @splat(1), 1, pin, 0, 3));
        try std.testing.expectError(error.RetainedEffectsFenceMismatch, retained_effects.acknowledge(&txn, @splat(1), 1, other, 0, 1));
        try retained_effects.acknowledge(&txn, @splat(1), 1, pin, 0, 2);
        // Idempotent admission returns original cut, not advanced acknowledgement.
        try std.testing.expectEqual(@as(u64, 0), try retained_effects.admit(&txn, @splat(1), 1, pin, retained_effects.default_limit));
        try std.testing.expectEqual(@as(usize, 0), try retained_effects.reclaim(&txn, @splat(1), 1, 1));
        try retained_effects.acknowledge(&txn, @splat(1), 2, other, 0, 2);
        try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaim(&txn, @splat(1), 1, 1));
        try std.testing.expectEqual(@as(u64, 1), (try retained_effects.load(&txn)).?.reclaimed);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaim(&txn, @splat(1), 128, 1024));
        try std.testing.expectEqual(@as(u64, 0), (try retained_effects.load(&txn)).?.retained_bytes);
        try retained_effects.release(&txn, @splat(1), 1, pin);
        try retained_effects.release(&txn, @splat(1), 1, pin);
        try retained_effects.release(&txn, @splat(1), 2, other);
        try std.testing.expectError(error.RetainedEffectsFenceMismatch, retained_effects.admit(&txn, @splat(1), 1, pin, retained_effects.default_limit));
        try txn.commit();
    }
    try store.put(a, "after release");
    try std.testing.expectError(error.NotFound, store.get(alloc, &retained_effects.recordKey(3)));
}

test "docstore retained row effects bound admission and abort oversized atomic writes" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.put(&internal_keys.identity_namespace_key, &@as(retained_effects.Namespace, @splat(1)));
    const key = try internal_keys.documentKeyAlloc(alloc, "large");
    defer alloc.free(key);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try retained_effects.admit(&txn, @splat(1), 1, @splat(1), retained_effects.max_frame_bytes);
        try txn.commit();
    }
    const value = try alloc.alloc(u8, retained_effects.max_frame_bytes);
    defer alloc.free(value);
    @memset(value, 'x');
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(key, value);
        try std.testing.expectError(error.RetainedEffectsFull, txn.commit());
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    try std.testing.expectError(error.NotFound, store.get(alloc, key));
    try std.testing.expectError(error.NotFound, store.get(alloc, &retained_effects.recordKey(1)));
    try store.put(key, "fits");
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.RetainedEffectsFull, retained_effects.admit(&txn, @splat(1), 2, @splat(2), retained_effects.max_frame_bytes));
        try txn.put(key, "ordinary write can still use admitted space");
        try std.testing.expectError(error.RetainedEffectsMixedControl, retained_effects.acknowledge(&txn, @splat(1), 1, @splat(1), 0, 1));
    }
}

test "docstore range tracking native LSM common prefix batch benchmark" {
    const range_protection = @import("range_protection.zig");
    const alloc = std.testing.allocator;
    var keys: [32][]u8 = undefined;
    var writes: [32]KVPair = undefined;
    var initialized: usize = 0;
    defer for (keys[0..initialized]) |key| alloc.free(key);
    for (&keys, &writes, 0..) |*key, *write, i| {
        var raw: [32]u8 = undefined;
        key.* = try internal_keys.documentKeyAlloc(alloc, try std.fmt.bufPrint(&raw, "doc:{d}", .{i}));
        initialized += 1;
        write.* = .{ .key = key.*, .value = "{\"value\":123,\"category\":\"bounded shared-prefix benchmark\"}" };
    }
    inline for (.{ false, true }) |active| {
        var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 4096 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        if (active) try store.put(range_protection.activation_key, range_protection.activation_value);
        const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        for (0..100) |_| try store.putBatch(&writes, &.{});
        const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(?u64, if (active) 100 else null), try range_protection.generation(&read, range_protection.bucket("doc:0")));
        std.debug.print("native LSM range tracking active={any} batches=100 rows_per_batch=32 elapsed_ns={d}\n", .{ active, elapsed });
    }
}

test "docstore range tracking survives native LSM reopen and rejects generation overflow atomically" {
    const range_protection = @import("range_protection.zig");
    const index_records = @import("db/relational_index_records.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const key = try internal_keys.documentKeyAlloc(alloc, "doc:one");
    defer alloc.free(key);
    const id = range_protection.bucket("doc:one");
    var component: std.ArrayList(u8) = .empty;
    defer component.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&component, alloc, "doc:one");
    var forward: std.ArrayList(u8) = .empty;
    defer forward.deinit(alloc);
    const forward_prefix = try index_records.forwardPrefix(.{ .generation = 7, .slot = 2 });
    try forward.appendSlice(alloc, &forward_prefix);
    try forward.appendSlice(alloc, &.{ 0x80, 'x', 0, 0 });
    try forward.appendSlice(alloc, component.items[1..]);
    var footer: [4]u8 = undefined;
    std.mem.writeInt(u32, &footer, @intCast(component.items.len - 1), .big);
    try forward.appendSlice(alloc, &footer);
    const span = (try range_protection.indexSpanDigest(forward.items)).?;
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 4096 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(range_protection.activation_key, range_protection.activation_value);
        try store.put(key, "before restart");
        try store.put(forward.items, "");
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 4096 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(key, "after restart");
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(?u64, 2), try range_protection.generation(&read, id));
        try std.testing.expectEqual(@as(?u64, 1), try range_protection.indexGeneration(&read, span));
        const counter_key = range_protection.counterKey(id);
        const index_counter_key = range_protection.indexCounterKey(span);
        var exhausted: [8]u8 = undefined;
        std.mem.writeInt(u64, &exhausted, std.math.maxInt(u64), .little);
        try store.put(&counter_key, &exhausted);
        try store.put(&index_counter_key, &exhausted);
        try std.testing.expectError(error.RangeTrackingGenerationExhausted, store.put(key, "must never publish"));
        try std.testing.expectError(error.RangeTrackingGenerationExhausted, store.put(forward.items, "must never publish"));
        var current = try store.beginReadTxn();
        defer current.abort();
        try std.testing.expectEqualStrings("after restart", try current.get(key));
        try std.testing.expectEqualStrings("", try current.get(forward.items));
        try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), try range_protection.generation(&current, id));
        try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), try range_protection.indexGeneration(&current, span));
    }
}

test "docstore retained row effects resume across LSM reopen and keep aborted GC history" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const key = try internal_keys.documentKeyAlloc(alloc, "row");
    defer alloc.free(key);
    const timestamp_key = try internal_keys.ttlKeyAlloc(alloc, "row");
    defer alloc.free(timestamp_key);
    const pin: [32]u8 = @splat(3);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &@as(retained_effects.Namespace, @splat(1)));
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            _ = try retained_effects.admit(&txn, @splat(1), 3, pin, retained_effects.default_limit);
            try txn.commit();
        }
        var timestamp: [8]u8 = undefined;
        std.mem.writeInt(u64, &timestamp, 111, .little);
        try store.putBatch(&.{ .{ .key = key, .value = "first epoch row bytes" }, .{ .key = timestamp_key, .value = &timestamp } }, &.{});
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        // A fresh DocStore discovers retention before its first primary write.
        try store.delete(key);
        {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            var first = (try retained_effects.read(&txn, @splat(1), 3, pin, 0)).?;
            const first_effect = (try first.next()).?;
            try std.testing.expectEqualStrings("first epoch row bytes", first_effect.value.?);
            try std.testing.expectEqual(@as(u64, 111), first_effect.timestamp);
            var second = (try retained_effects.read(&txn, @splat(1), 3, pin, 1)).?;
            const deleted = (try second.next()).?;
            try std.testing.expect(deleted.value == null);
            try std.testing.expectEqual(@as(u64, 0), deleted.timestamp);
            try std.testing.expect(try retained_effects.read(&txn, @splat(1), 3, pin, 2) == null);
        }
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            try retained_effects.acknowledge(&txn, @splat(1), 3, pin, 0, 2);
            try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaim(&txn, @splat(1), 1, 1));
        }
        {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            const state = (try retained_effects.load(&txn)).?;
            try std.testing.expectEqual(@as(u64, 0), state.reclaimed);
            try std.testing.expect(try retained_effects.read(&txn, @splat(1), 3, pin, 0) != null);
        }
    }
}

test "docstore retained REF5 keeps an oversized after-image across reopen and reclaims bounded chunks" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const key = try internal_keys.documentKeyAlloc(alloc, "large-row");
    defer alloc.free(key);
    const value = try alloc.alloc(u8, 17 * 1024 * 1024);
    defer alloc.free(value);
    @memset(value, 'x');
    const pin: [32]u8 = @splat(7);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &@as(retained_effects.Namespace, @splat(3)));
        var admit_txn = try store.beginWriteTxn();
        errdefer admit_txn.abort();
        _ = try retained_effects.admitWithCapabilities(&admit_txn, @splat(3), 1, pin, retained_effects.default_limit, false, true);
        try admit_txn.commit();
        try store.put(key, value);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        const scratch = try alloc.alloc(u8, @import("retained_frame.zig").chunk_bytes);
        defer alloc.free(scratch);
        var cache: @import("retained_frame.zig").View.ChunkCache = .{ .bytes = scratch };
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            const frame = (try retained_effects.readFrame(&read, @splat(3), 1, pin, 0, &cache)).?;
            const view = switch (frame) {
                .chunked => |v| v,
                .contiguous => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(@as(u32, 1), view.effect_count);
            const effect = try view.effectAt(0, &cache);
            try std.testing.expectEqual(@as(?u32, @intCast(value.len)), effect.value_len);
            var prefix: [4]u8 = undefined;
            try std.testing.expectEqual(prefix.len, try view.readAt(effect.value_offset, &prefix, &cache));
            try std.testing.expectEqualSlices(u8, "xxxx", &prefix);
            var suffix: [4]u8 = undefined;
            try std.testing.expectEqual(suffix.len, try view.readAt(effect.value_offset + @as(u32, @intCast(value.len)) - 4, &suffix, &cache));
            try std.testing.expectEqualSlices(u8, "xxxx", &suffix);
        }
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try retained_effects.acknowledge(&txn, @splat(3), 1, pin, 0, 1);
            try txn.commit();
        }
        var complete = false;
        for (0..32) |_| {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            _ = try retained_effects.reclaim(&txn, @splat(3), 1, 1);
            const state = (try retained_effects.load(&txn)).?;
            complete = state.reclaimed == 1;
            if (complete) try std.testing.expectEqual(@as(u64, 0), state.retained_bytes);
            try txn.commit();
            if (complete) break;
        }
        try std.testing.expect(complete);
    }
}

test "docstore retained REF5 graph language persists exact value and tombstone across reopen" {
    const alloc = std.testing.allocator;
    const retained = @import("retained_effects.zig");
    const frame = @import("retained_frame.zig");
    const codec = @import("db/enrichment/artifact_codec.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc", "g", "links", "target");
    defer alloc.free(key);
    const value = try codec.encodeGraphEdgeAlloc(alloc, null, 7, 1, 0, 0, "");
    defer alloc.free(value);
    const pin: [32]u8 = @splat(9);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &@as(retained.Namespace, @splat(3)));
        var admit = try store.beginWriteTxn();
        errdefer admit.abort();
        _ = try retained.admitWithArtifactCapabilities(&admit, @splat(3), 1, pin, retained.default_limit, true, true, true);
        try admit.commit();
        var put = try store.beginWriteTxn();
        errdefer put.abort();
        try put.put(&internal_keys.ordered_document_applied_entry_key, "entry-1");
        try put.put(key, value);
        try put.commit();
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        const scratch = try alloc.alloc(u8, frame.chunk_bytes);
        defer alloc.free(scratch);
        var cache: frame.View.ChunkCache = .{ .bytes = scratch };
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            const stored = (try retained.readFrame(&read, @splat(3), 1, pin, 0, &cache)).?;
            const view = switch (stored) {
                .chunked => |item| item,
                .contiguous => return error.TestUnexpectedResult,
            };
            try std.testing.expect(view.graph_artifacts);
            const effect = try view.effectAt(0, &cache);
            const copied = try alloc.alloc(u8, effect.value_len.?);
            defer alloc.free(copied);
            try std.testing.expectEqual(copied.len, try view.readAt(effect.value_offset, copied, &cache));
            try std.testing.expectEqualSlices(u8, value, copied);
        }
        var remove = try store.beginWriteTxn();
        errdefer remove.abort();
        try remove.put(&internal_keys.ordered_document_applied_entry_key, "entry-2");
        try remove.delete(key);
        try remove.commit();
        var read = try store.beginReadTxn();
        defer read.abort();
        const removed = (try retained.readFrame(&read, @splat(3), 1, pin, 1, &cache)).?;
        const view = switch (removed) {
            .chunked => |item| item,
            .contiguous => return error.TestUnexpectedResult,
        };
        try std.testing.expectEqual(@as(?u32, null), (try view.effectAt(0, &cache)).value_len);
    }
}

test "docstore retained integrity and row effects share an immutable atomic frame across reopen" {
    const alloc = std.testing.allocator;
    const integrity = @import("db/relational_integrity_contract.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/integrity-tail", .{tmp.sub_path});
    defer alloc.free(path);
    const primary = try internal_keys.documentKeyAlloc(alloc, "row");
    defer alloc.free(primary);
    const address = try integrity.Address.init(@splat(1), "parent");
    const claim_key = address.claimKey();
    const claim: integrity.Claim = .{ .tuple = "parent", .parent_table = "parents", .parent_key = "row", .schema_version = 1 };
    const claim_bytes = try claim.encode(alloc, address);
    defer alloc.free(claim_bytes);
    const reference: integrity.Reference = .{ .child_table = "children", .child_key = "child", .constraint_name = "fk", .constraint_generation = @splat(2) };
    const reference_key = try reference.key(address);
    const reference_bytes = try reference.encode(alloc, address);
    defer alloc.free(reference_bytes);
    const namespace: retained_effects.Namespace = @splat(1);
    const pin: [32]u8 = @splat(2);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &namespace);
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            _ = try retained_effects.admit(&txn, namespace, 1, pin, retained_effects.default_limit);
            try txn.commit();
        }
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(primary, "first");
            try txn.put(&claim_key, claim_bytes);
            try txn.put(&reference_key, reference_bytes);
            try txn.commit();
        }
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            try txn.delete(&claim_key);
            try txn.delete(primary);
        }
        // Integrity-only edits are retained even without a primary rewrite;
        // coalescing records final values, not intermediate put/delete order.
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try txn.delete(&claim_key);
            try txn.put(&claim_key, claim_bytes);
            try txn.delete(&reference_key);
            try txn.commit();
        }
        // Invalid source metadata cannot produce an unreadable committed log.
        try std.testing.expectError(error.RetainedEffectsCorrupt, store.put(&claim_key, "corrupt"));
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var first = (try retained_effects.read(&txn, namespace, 1, pin, 0)).?;
        const saved_claim = (try first.next()).?;
        try std.testing.expect(saved_claim.isIntegrity());
        try std.testing.expectEqualSlices(u8, &claim_key, saved_claim.key);
        try std.testing.expectEqualSlices(u8, claim_bytes, saved_claim.value.?);
        try std.testing.expectEqual(@as(u64, 0), saved_claim.timestamp);
        const saved_reference = (try first.next()).?;
        try std.testing.expectEqualSlices(u8, &reference_key, saved_reference.key);
        try std.testing.expectEqualSlices(u8, reference_bytes, saved_reference.value.?);
        const saved_row = (try first.next()).?;
        try std.testing.expect(!saved_row.isIntegrity());
        try std.testing.expectEqualStrings("first", saved_row.value.?);
        try std.testing.expect(try first.next() == null);
        var second = (try retained_effects.read(&txn, namespace, 1, pin, 1)).?;
        try std.testing.expectEqualSlices(u8, claim_bytes, (try second.next()).?.value.?);
        const deleted = (try second.next()).?;
        try std.testing.expectEqualSlices(u8, &reference_key, deleted.key);
        try std.testing.expect(deleted.value == null);
        try std.testing.expect(try second.next() == null);
        try std.testing.expect(try retained_effects.read(&txn, namespace, 1, pin, 2) == null);
        try std.testing.expectEqualSlices(u8, claim_bytes, try txn.get(&claim_key));
    }
}

test "docstore retained document timestamps remain paired with overwritten afterimages across reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const primary = try internal_keys.documentKeyAlloc(alloc, "row\x00binary");
    defer alloc.free(primary);
    const timestamp_key = try internal_keys.ttlKeyAlloc(alloc, "row\x00binary");
    defer alloc.free(timestamp_key);
    const namespace: retained_effects.Namespace = @splat(1);
    const pin: [32]u8 = @splat(2);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &namespace);
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            _ = try retained_effects.admit(&txn, namespace, 1, pin, retained_effects.default_limit);
            try txn.commit();
        }
        for ([_]u64{ 111, 222 }) |timestamp| {
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, timestamp, .little);
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(primary, if (timestamp == 111) "first" else "second");
            try txn.put(timestamp_key, &encoded);
            try txn.commit();
        }
        var timestamp_only: [8]u8 = undefined;
        std.mem.writeInt(u64, &timestamp_only, 333, .little);
        try store.put(timestamp_key, &timestamp_only);
        {
            var aborted = try store.beginWriteTxn();
            defer aborted.abort();
            std.mem.writeInt(u64, &timestamp_only, 444, .little);
            try aborted.put(timestamp_key, &timestamp_only);
        }
        const missing_timestamp = try internal_keys.ttlKeyAlloc(alloc, "not-a-row");
        defer alloc.free(missing_timestamp);
        try store.put(missing_timestamp, &timestamp_only);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        var txn = try store.beginReadTxn();
        defer txn.abort();
        for (0..3) |after| {
            var frame = (try retained_effects.read(&txn, namespace, 1, pin, after)).?;
            const effect = (try frame.next()).?;
            try std.testing.expectEqualStrings(if (after == 0) "first" else "second", effect.value.?);
            try std.testing.expectEqual(@as(u64, (after + 1) * 111), effect.timestamp);
            try std.testing.expectEqualStrings(primary, effect.key);
            try std.testing.expect(try frame.next() == null);
        }
        try std.testing.expect(try retained_effects.read(&txn, namespace, 1, pin, 3) == null);
    }
}

test "docstore retained row effects fence foreign native adoption without blocking target writes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const source_namespace: retained_effects.Namespace = @splat(1);
    const target_namespace: retained_effects.Namespace = @splat(2);
    const pin: [32]u8 = @splat(3);
    const key = try internal_keys.documentKeyAlloc(alloc, "source");
    defer alloc.free(key);
    const target_key = try internal_keys.documentKeyAlloc(alloc, "target");
    defer alloc.free(target_key);
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        try store.put(&internal_keys.identity_namespace_key, &source_namespace);
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try retained_effects.admit(&txn, source_namespace, 1, pin, retained_effects.max_frame_bytes);
        try txn.commit();
        // Two large frames leave insufficient capacity for another small
        // mutation. Copied consumers must not impose that source pressure on
        // a target with a new namespace.
        const value = try alloc.alloc(u8, retained_effects.max_frame_bytes / 2 - 128);
        defer alloc.free(value);
        @memset(value, 'x');
        try store.put(key, value);
        try store.put(key, value);
    }
    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            // Same logical namespace survives native transfer/reopen.
            try std.testing.expect(try retained_effects.read(&txn, @splat(1), 1, pin, 0) != null);
        }
        const mutation = @as([2048]u8, @splat('t'));
        try std.testing.expectError(error.RetainedEffectsFull, store.put(target_key, &mutation));
        {
            // Native namespace adoption persists identity before target rows.
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(&internal_keys.identity_namespace_key, &target_namespace);
            try txn.put(target_key, &mutation);
            try txn.commit();
        }
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            const state = (try retained_effects.load(&txn)).?;
            try std.testing.expectEqual(@as(u64, 2), state.latest);
            try std.testing.expectEqualSlices(u8, &source_namespace, &state.namespace);
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.read(&txn, @splat(1), 1, pin, 0));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.acknowledge(&txn, @splat(1), 1, pin, 0, 2));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.release(&txn, @splat(1), 1, pin));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.reclaim(&txn, @splat(1), 1, 1));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.admit(&txn, target_namespace, 2, @splat(4), retained_effects.max_frame_bytes));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.reclaimForeign(&txn, target_namespace, target_namespace, 1, 1));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.reclaimForeign(&txn, target_namespace, @splat(9), 1, 1));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.reclaimForeign(&txn, @splat(9), source_namespace, 1, 1));
        }
        {
            // In particular, a disabled foreign capture cannot become a
            // matching source after already missing a primary mutation.
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            try txn.put(target_key, "must abort");
            try std.testing.expectError(error.RetainedEffectsMixedControl, txn.put(&internal_keys.identity_namespace_key, &source_namespace));
            try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
        }
        const target_value = try store.get(alloc, target_key);
        defer alloc.free(target_value);
        try std.testing.expectEqualSlices(u8, &mutation, target_value);
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaimForeign(&txn, target_namespace, source_namespace, 1, 1));
        }
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try std.testing.expectEqual(@as(u64, 0), (try retained_effects.load(&txn)).?.reclaimed);
            try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaimForeign(&txn, target_namespace, source_namespace, 1, 1));
            try txn.commit();
        }
        try store.put(target_key, "target remains writable during bounded cleanup");
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            const state = (try retained_effects.load(&txn)).?;
            try std.testing.expectEqual(@as(u64, 1), state.reclaimed);
            try std.testing.expect(!state.active());
            try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaimForeign(&txn, target_namespace, source_namespace, 1, 1));
            try std.testing.expect(try retained_effects.load(&txn) == null);
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.admit(&txn, source_namespace, 1, pin, retained_effects.max_frame_bytes));
            // Epoch and pin may legitimately collide after adoption; the
            // explicit namespace must still fence every delayed old request.
            _ = try retained_effects.admit(&txn, target_namespace, 1, pin, retained_effects.max_frame_bytes);
            try txn.commit();
        }
        try store.put(target_key, "new scoped history");
        {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            const state = (try retained_effects.load(&txn)).?;
            try std.testing.expectEqualSlices(u8, &target_namespace, &state.namespace);
            try std.testing.expectEqual(@as(u64, 1), state.latest);
            var record = (try retained_effects.read(&txn, target_namespace, 1, pin, 0)).?;
            try std.testing.expectEqualStrings("new scoped history", (try record.next()).?.value.?);
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.read(&txn, source_namespace, 1, pin, 0));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.acknowledge(&txn, source_namespace, 1, pin, 0, 1));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.release(&txn, source_namespace, 1, pin));
            try std.testing.expectError(error.RetainedEffectsNamespaceMismatch, retained_effects.reclaim(&txn, source_namespace, 1, 1));
        }
    }
}

test "docstore retained row effects require durable identity but preserve unretained first-write initialization" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const key = try internal_keys.documentKeyAlloc(alloc, "first");
    defer alloc.free(key);
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    try std.testing.expectError(error.RetainedEffectsIdentityRequired, retained_effects.admit(&txn, @splat(1), 1, @splat(1), retained_effects.default_limit));
    try txn.put(key, "ordinary first row");
    try txn.put(&internal_keys.identity_namespace_key, &@as(retained_effects.Namespace, @splat(1)));
    try txn.commit();
    const value = try store.get(alloc, key);
    defer alloc.free(value);
    try std.testing.expectEqualStrings("ordinary first row", value);
}

test "docstore putBatch atomic" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    // Pre-populate
    try store.put("key_a", "old_a");
    try store.put("key_b", "old_b");

    // Batch: write two new, delete one old
    const writes = [_]KVPair{
        .{ .key = "key_c", .value = "val_c" },
        .{ .key = "key_d", .value = "val_d" },
    };
    const deletes = [_][]const u8{"key_a"};
    try store.putBatch(&writes, &deletes);

    // key_a deleted
    try std.testing.expectError(error.NotFound, store.get(std.testing.allocator, "key_a"));

    // key_b unchanged
    const b = try store.get(std.testing.allocator, "key_b");
    defer std.testing.allocator.free(b);
    try std.testing.expectEqualStrings("old_b", b);

    // New keys exist
    const c_val = try store.get(std.testing.allocator, "key_c");
    defer std.testing.allocator.free(c_val);
    try std.testing.expectEqualStrings("val_c", c_val);
}

test "docstore scanPrefix" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("user:1", "alice");
    try store.put("user:2", "bob");
    try store.put("user:3", "carol");
    try store.put("item:1", "widget");
    try store.put("item:2", "gadget");

    const users = try store.scanPrefix(std.testing.allocator, "user:");
    defer DocStore.freeResults(std.testing.allocator, users);

    try std.testing.expectEqual(@as(usize, 3), users.len);
    try std.testing.expectEqualStrings("user:1", users[0].key);
    try std.testing.expectEqualStrings("alice", users[0].value);
    try std.testing.expectEqualStrings("user:3", users[2].key);
}

test "docstore scanRange" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");
    try store.put("e", "5");

    // Range [b, d) should return b, c
    const results = try store.scanRange(std.testing.allocator, "b", "d");
    defer DocStore.freeResults(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("b", results[0].key);
    try std.testing.expectEqualStrings("c", results[1].key);
}

test "docstore scanRange unbounded upper" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");

    // Range [b, +inf) = empty upper bound
    const results = try store.scanRange(std.testing.allocator, "b", "");
    defer DocStore.freeResults(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("b", results[0].key);
    try std.testing.expectEqualStrings("c", results[1].key);
}

fn skipInternalMedianKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
        std.mem.startsWith(u8, key, "splitstate:") or
        std.mem.startsWith(u8, key, "splitdelta:");
}

test "docstore findMedianKey ignores internal keys" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "va");
    try store.put("b", "vb");
    try store.put("c", "vc");
    try store.put("d", "vd");
    try store.put("e", "ve");
    try store.put("\x00\x00__metadata__:schema", "meta");
    try store.put("splitstate:current", "state");
    try store.put("splitdelta:0001", "delta");

    const key = try store.findMedianKey(std.testing.allocator, "", "", .{ .skip_fn = &skipInternalMedianKey });
    defer std.testing.allocator.free(key);
    try std.testing.expectEqualStrings("c", key);
}

test "docstore findMedianKey respects range bounds" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "va");
    try store.put("b", "vb");
    try store.put("c", "vc");
    try store.put("d", "vd");
    try store.put("e", "ve");

    const key = try store.findMedianKey(std.testing.allocator, "b", "e", .{});
    defer std.testing.allocator.free(key);
    try std.testing.expectEqualStrings("c", key);
}

test "ByteRange.contains" {
    // [b, d)
    const range = ByteRange{ .start = "b", .end = "d" };
    try std.testing.expect(!range.contains("a"));
    try std.testing.expect(range.contains("b"));
    try std.testing.expect(range.contains("c"));
    try std.testing.expect(!range.contains("d"));
    try std.testing.expect(!range.contains("e"));

    // Unbounded: ["", "")
    const all = ByteRange{ .start = "", .end = "" };
    try std.testing.expect(all.contains("anything"));

    // Half-bounded: [c, "")
    const from_c = ByteRange{ .start = "c", .end = "" };
    try std.testing.expect(!from_c.contains("b"));
    try std.testing.expect(from_c.contains("c"));
    try std.testing.expect(from_c.contains("z"));
}

test "KeyEncoder edge key round-trip" {
    var buf: [512]u8 = undefined;
    const key = KeyEncoder.makeEdgeKey(&buf, "doc1", "graph_idx", "cites", "paper2");
    try std.testing.expectEqualStrings("doc1:i:graph_idx:out:cites:paper2:o", key);
    try std.testing.expect(KeyEncoder.isEdgeKey(key));

    const parsed = KeyEncoder.parseEdgeKey(key).?;
    try std.testing.expectEqualStrings("doc1", parsed.source);
    try std.testing.expectEqualStrings("graph_idx", parsed.index_name);
    try std.testing.expectEqualStrings("cites", parsed.edge_type);
    try std.testing.expectEqualStrings("paper2", parsed.target);
}

test "KeyEncoder reverse edge key round-trip" {
    var buf: [512]u8 = undefined;
    const key = KeyEncoder.makeReverseEdgeKey(&buf, "paper2", "graph_idx", "cites", "doc1");
    try std.testing.expectEqualStrings("paper2:i:graph_idx:in:cites:doc1:i", key);

    const parsed = KeyEncoder.parseEdgeKey(key).?;
    try std.testing.expectEqualStrings("doc1", parsed.source);
    try std.testing.expectEqualStrings("graph_idx", parsed.index_name);
    try std.testing.expectEqualStrings("cites", parsed.edge_type);
    try std.testing.expectEqualStrings("paper2", parsed.target);
}

test "KeyEncoder enrichment keys" {
    var buf: [512]u8 = undefined;

    const emb = KeyEncoder.makeEmbeddingKey(&buf, "doc1", "emb_idx");
    try std.testing.expectEqualStrings("doc1:i:emb_idx:e", emb);

    const sum = KeyEncoder.makeSummaryKey(&buf, "doc1", "sum_idx");
    try std.testing.expectEqualStrings("doc1:i:sum_idx:s", sum);

    const chunk = KeyEncoder.makeChunkKey(&buf, "doc1", "chunk_idx", 42);
    try std.testing.expectEqualStrings("doc1:i:chunk_idx:42:c", chunk);

    const e_prefix = KeyEncoder.makeEnrichmentPrefix(&buf, "doc1", "chunk", "body_chunks_v1");
    try std.testing.expectEqualStrings("doc1:e:chunk:body_chunks_v1:", e_prefix);

    const e_root_prefix = KeyEncoder.makeEnrichmentRootPrefix(&buf, "doc1");
    try std.testing.expectEqualStrings("doc1:e:", e_root_prefix);

    const e_type_prefix = KeyEncoder.makeEnrichmentTypePrefix(&buf, "doc1", "chunk");
    try std.testing.expectEqualStrings("doc1:e:chunk:", e_type_prefix);

    const e_chunk = KeyEncoder.makeEnrichmentChunkKey(&buf, "doc1", "body_chunks_v1", 42);
    try std.testing.expectEqualStrings("doc1:e:chunk:body_chunks_v1:42", e_chunk);

    const e_emb = KeyEncoder.makeEnrichmentEmbeddingKey(&buf, "doc1", "body_dense_v1");
    try std.testing.expectEqualStrings("doc1:e:embedding:body_dense_v1", e_emb);

    const chunk_emb = KeyEncoder.makeEnrichmentEmbeddingKey(&buf, "doc1:e:chunk:body_chunks_v1:42", "body_dense_v1");
    try std.testing.expectEqualStrings("doc1:e:chunk:body_chunks_v1:42:e:embedding:body_dense_v1", chunk_emb);
}

test "KeyEncoder range sentinels" {
    var buf: [512]u8 = undefined;

    const start = KeyEncoder.keyRangeStart(&buf, "doc1");
    try std.testing.expectEqual(@as(usize, 6), start.len);
    try std.testing.expectEqualStrings("doc1:", start[0..5]);
    try std.testing.expectEqual(@as(u8, 0x00), start[5]);

    var buf2: [512]u8 = undefined;
    const end_key = KeyEncoder.keyRangeEnd(&buf2, "doc1");
    try std.testing.expectEqual(@as(usize, 6), end_key.len);
    try std.testing.expectEqualStrings("doc1:", end_key[0..5]);
    try std.testing.expectEqual(@as(u8, 0xFF), end_key[5]);
}

test "docstore streaming scan visits all keys" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");
    try store.put("e", "5");

    const Context = struct {
        var count: usize = 0;
        var last_key: [1]u8 = undefined;
        fn cb(key: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
            count += 1;
            last_key[0] = key[0];
            return .@"continue";
        }
    };
    Context.count = 0;

    // Scan [b, d) — should visit b, c
    try store.scan("b", "d", .{}, &Context.cb);
    try std.testing.expectEqual(@as(usize, 2), Context.count);
    try std.testing.expectEqual(@as(u8, 'c'), Context.last_key[0]);
}

test "docstore streaming scan supports reverse bounded ranges" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");

    const Context = struct {
        var keys: [3]u8 = undefined;
        var count: usize = 0;

        fn cb(key: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
            keys[count] = key[0];
            count += 1;
            return .@"continue";
        }
    };
    Context.count = 0;

    try store.scan("a", "d", .{ .reverse = true }, &Context.cb);
    try std.testing.expectEqual(@as(usize, 3), Context.count);
    try std.testing.expectEqual(@as(u8, 'c'), Context.keys[0]);
    try std.testing.expectEqual(@as(u8, 'b'), Context.keys[1]);
    try std.testing.expectEqual(@as(u8, 'a'), Context.keys[2]);
}

test "docstore streaming scan skip_fn" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("aa", "1");
    try store.put("ab", "2");
    try store.put("ba", "3");
    try store.put("bb", "4");

    const Context = struct {
        var count: usize = 0;
        fn skipB(key: []const u8) bool {
            return key[0] == 'b';
        }
        fn cb(_: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
            count += 1;
            return .@"continue";
        }
    };
    Context.count = 0;

    // Scan all, skip keys starting with 'b'
    try store.scan("a", "", .{ .skip_fn = &Context.skipB }, &Context.cb);
    try std.testing.expectEqual(@as(usize, 2), Context.count); // only aa, ab
}

test "docstore streaming scan stop early" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");

    const Context = struct {
        var count: usize = 0;
        fn cb(_: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
            count += 1;
            if (count >= 2) return .stop;
            return .@"continue";
        }
    };
    Context.count = 0;

    try store.scan("a", "", .{}, &Context.cb);
    try std.testing.expectEqual(@as(usize, 2), Context.count);
}

test "docstore reopen preserves data" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    // Write data
    {
        var store = try DocStore.open(std.testing.allocator, path, .{});
        defer store.close();
        try store.put("persist_key", "persist_val");
    }

    // Reopen and verify
    {
        var store = try DocStore.open(std.testing.allocator, path, .{});
        defer store.close();
        const val = try store.get(std.testing.allocator, "persist_key");
        defer std.testing.allocator.free(val);
        try std.testing.expectEqualStrings("persist_val", val);
    }
}

test "docstore releases payload policy before runtime writer admission" {
    const Gate = struct {
        entered: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(bool) = .init(false),

        fn wait(self: *@This()) void {
            self.entered.store(true, .release);
            while (!self.released.load(.acquire)) std.Thread.yield() catch {};
        }
    };
    const Intercept = struct {
        gate: *Gate,

        fn beginWrite(_: Allocator, ptr: *anyopaque) anyerror!backend_erased.WriteTxn {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.gate.wait();
            return error.TestWriterAdmissionReleased;
        }

        fn beginBatch(_: Allocator, ptr: *anyopaque) anyerror!backend_erased.Batch {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.gate.wait();
            return error.TestWriterAdmissionReleased;
        }

        pub fn beginBatchWithOptions(alloc: Allocator, ptr: *anyopaque, _: backend_types.BatchOptions) anyerror!backend_erased.Batch {
            return beginBatch(alloc, ptr);
        }
    };
    const Mode = enum { transaction, batch };
    const Runner = struct {
        store: *DocStore,
        mode: Mode,
        done: std.atomic.Value(bool) = .init(false),
        result: ?anyerror = null,

        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            switch (self.mode) {
                .transaction => {
                    var txn = self.store.beginWriteTxn() catch |err| {
                        self.result = err;
                        return;
                    };
                    txn.abort();
                },
                .batch => {
                    var batch = self.store.beginWriteBatchWithOptions(.{}) catch |err| {
                        self.result = err;
                        return;
                    };
                    batch.abort();
                },
            }
            self.result = error.TestUnexpectedWriterAdmission;
        }
    };

    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();

    const original_ptr = store.runtime_store.ptr;
    const original_vtable = store.runtime_store.vtable;
    defer {
        store.runtime_store.ptr = original_ptr;
        store.runtime_store.vtable = original_vtable;
    }
    var intercepted_vtable = original_vtable.*;
    intercepted_vtable.begin_write = Intercept.beginWrite;
    intercepted_vtable.begin_batch = Intercept.beginBatch;
    intercepted_vtable.begin_batch_with_options = Intercept.beginBatchWithOptions;
    store.runtime_store.vtable = &intercepted_vtable;

    inline for (.{ Mode.transaction, Mode.batch }) |mode| {
        var gate = Gate{};
        var intercept = Intercept{ .gate = &gate };
        store.runtime_store.ptr = &intercept;
        var runner = Runner{ .store = &store, .mode = mode };
        const thread = try std.Thread.spawn(.{}, Runner.run, .{&runner});
        while (!gate.entered.load(.acquire) and !runner.done.load(.acquire))
            std.Thread.yield() catch {};
        const reached_backend = gate.entered.load(.acquire);
        const policy_available = reached_backend and store.payload_policy_mutex.tryLock();
        if (policy_available) store.unlockPayloadPolicy();
        gate.released.store(true, .release);
        thread.join();
        try std.testing.expect(reached_backend);
        try std.testing.expect(policy_available);
        try std.testing.expectEqual(error.TestWriterAdmissionReleased, runner.result.?);
    }
}

test "docstore runtime lsm exposes large replaying graph artifact prefix batch immediately after commit" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var backend = try lsm_backend.Backend.open(alloc, path, .{
        .flush_threshold_bytes = 32 * 1024 * 1024,
    });
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer writes.deinit(alloc);
    try writes.ensureTotalCapacity(alloc, 1500);

    var i: usize = 0;
    while (i < 1500) : (i += 1) {
        const target = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
        defer alloc.free(target);
        const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:0000", "gr_v1", "links", target);
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"target\":\"{s}\"}}", .{target});
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    defer {
        for (writes.items) |kv| {
            alloc.free(@constCast(kv.key));
            alloc.free(@constCast(kv.value));
        }
    }

    try store.putBatchWithReplay(null, writes.items, &.{}, .{
        .sequence = 1,
        .payload = "replay:graph",
    });

    const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:0000", "gr_v1");
    defer alloc.free(prefix);
    const results = try store.scanPrefix(alloc, prefix);
    defer DocStore.freeResults(alloc, results);
    try std.testing.expectEqual(@as(usize, 1500), results.len);
}

test "docstore backend adapters expose txn cursor and batch operations" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put("doc:a", "1");
        var cur = try write.openCursor();
        try std.testing.expectEqualStrings("doc:a", (try cur.start(.{})).?.key);
        cur.close();
        try write.commit();
    }

    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("1", try read.get("doc:a"));
    }

    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        var batch_adapter = batch.adapter();
        try batch_adapter.put("doc:b", "2");
        try std.testing.expectEqualStrings("2", try batch_adapter.get("doc:b"));
        try batch_adapter.commit();
    }

    const results = try store.scanPrefix(std.testing.allocator, "doc:");
    defer DocStore.freeResults(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 2), results.len);
}

test "docstore backend store opens concrete txn and batch handles" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    var backend = store.backendStore();
    try std.testing.expect(backend.capabilities().cursors);
    try std.testing.expectEqual(backend_types.WriteBatchMode.atomic, backend.capabilities().write_batches);

    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put("doc:x", "9");
        try write.commit();
    }

    {
        var txn = try backend.beginRead();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("9", try read.get("doc:x"));
    }

    {
        var batch = try backend.beginBatch();
        errdefer batch.abort();
        var batch_adapter = batch.adapter();
        try batch_adapter.put("doc:y", "10");
        try batch_adapter.commit();
    }
}

test "docstore backend runtime erases store handles" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    var runtime = try backend_erased.storeFrom(std.testing.allocator, store.backendStore());
    defer runtime.deinit();
    try std.testing.expect(runtime.capabilities().cursors);

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:r", "11");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("11", try txn.get("doc:r"));
        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("doc:r", (try cur.seekAtOrAfter("doc:r")).?.key);
    }
}

test "ordered artifact inventory erased document snapshots retain their cut across fork and parent close" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.putBatch(&.{.{ .key = "doc", .value = "old" }}, &.{});
    var runtime = try backend_erased.storeFrom(alloc, store.backendStore());
    defer runtime.deinit();
    var parent = try runtime.beginRead();
    var parent_open = true;
    defer if (parent_open) parent.abort();
    var child = try parent.forkRead();
    defer child.abort();
    parent.abort();
    parent_open = false;
    try store.putBatch(&.{.{ .key = "doc", .value = "new" }}, &.{});
    for (0..128) |_| {
        const next = try child.forkRead();
        child.abort();
        child = next;
        try std.testing.expectEqualStrings("old", try child.get("doc"));
        var cursor = try child.openCursor();
        defer cursor.close();
        try std.testing.expectEqualStrings("old", (try cursor.seekAtOrAfter("doc")).?.value);
    }
    var current = try runtime.beginRead();
    defer current.abort();
    try std.testing.expectEqualStrings("new", try current.get("doc"));
}

test "ordered artifact inventory replay reservations do not publish a committed cut" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.ensureReplayIndexInitialized();
    const abandoned = store.reserveNextReplaySequence(1);
    const committed = store.reserveNextReplaySequence(1);
    try std.testing.expectEqual(@as(u64, 1), abandoned);
    try std.testing.expectEqual(@as(u64, 2), committed);
    try std.testing.expectEqual(@as(u64, 0), store.lastReplaySequence(0));
    try store.appendReplayOpaque(alloc, committed, "committed");
    try std.testing.expectEqual(committed, store.lastReplaySequence(0));
    try std.testing.expectError(error.InvalidBatchRequest, store.putBatchWithReplay(null, &.{.{ .key = "late", .value = "must abort" }}, &.{}, .{ .sequence = abandoned, .payload = "late" }));
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.NotFound, read.get("late"));
        try std.testing.expectEqual(committed, try store.lastReplaySequenceFromTxn(&read, 0));
    }
    const entries = try store.iterateReplayFrom(alloc, abandoned);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(committed, entries[0].sequence);
    _ = store.reserveNextReplaySequence(1);
    _ = store.reserveNextReplaySequence(1);
    try std.testing.expectEqual(committed, store.lastReplaySequence(0));
    // A durable split/rebuild floor must not be suppressed by a higher
    // process-local reservation. Reopening reloads only the durable cut.
    try store.ensureReplayNextSequenceAtLeast(4);
    try std.testing.expectEqual(@as(u64, 3), store.lastReplaySequence(0));
    var reopened = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 3), reopened.lastReplaySequence(0));
    try std.testing.expectEqual(@as(u64, 4), reopened.nextReplaySequence(1));
}

test "ordered artifact inventory ambiguous replay commit reloads durable cut without reusing reservation" {
    const Fault = struct {
        inner: backend_erased.Batch,
        committed: bool = false,
        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn commit(_: Allocator, ptr: *anyopaque) !void {
            const self = cast(ptr);
            try self.inner.commit();
            self.committed = true;
            return error.TestCommitReplyLost;
        }
        fn abort(_: Allocator, ptr: *anyopaque) void {
            const self = cast(ptr);
            if (!self.committed) self.inner.abort();
        }
        fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
            return cast(ptr).inner.get(key);
        }
        fn put(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
            return cast(ptr).inner.put(key, value);
        }
        fn delete(ptr: *anyopaque, key: []const u8) !void {
            return cast(ptr).inner.delete(key);
        }
        const vtable: backend_erased.Batch.VTable = .{ .commit = commit, .abort = abort, .get = get, .put = put, .delete = delete };
    };
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.ensureReplayIndexInitialized();
    try std.testing.expectEqual(@as(u64, 0), store.lastReplaySequence(0));
    const reserved = store.reserveNextReplaySequence(1);
    var batch = try store.beginWriteBatch();
    var fault: Fault = .{ .inner = batch.runtime.? };
    batch.runtime = .{ .allocator = alloc, .ptr = &fault, .vtable = &Fault.vtable };
    try batch.setReplayOpaque(reserved, "committed despite lost reply");
    try std.testing.expectError(error.TestCommitReplyLost, batch.commit());
    batch.abort();
    try std.testing.expect(fault.committed);
    try std.testing.expectEqual(reserved, store.lastReplaySequence(0));
    try std.testing.expectEqual(reserved + 1, store.reserveNextReplaySequence(1));
}

test "docstore replay rows use replay keyspace" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.putBatchWithReplay(null, &.{
        .{ .key = "doc:a", .value = "A" },
    }, &.{}, .{
        .sequence = 1,
        .payload = "replay:1",
    });

    try std.testing.expectEqual(@as(u64, 1), store.lastReplaySequence(0));
    try std.testing.expectEqual(@as(u64, 2), store.nextReplaySequence(1));

    const entries = try store.iterateReplayFrom(std.testing.allocator, 1);
    defer {
        for (entries) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("replay:1", entries[0].payload);

    try store.truncateReplayUpTo(std.testing.allocator, 1);
    const remaining = try store.iterateReplayFrom(std.testing.allocator, 1);
    defer {
        for (remaining) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(remaining);
    }
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "docstore replay retirement pages keys within bounded memory and preserves unacknowledged rows" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    const cutoff: u64 = DocStore.replay_truncate_batch_keys * 2 + 17;
    const hints = [_]u8{ internal_keys.replay_all_kind, replayHintOrdinal(.dense_vector) };
    var sequence: u64 = 1;
    while (sequence <= cutoff + 1) {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(&internal_keys.replay_meta_init_key, "1");
        const end = @min(sequence + 1024, cutoff + 2);
        while (sequence < end) : (sequence += 1) {
            const payload = try change_journal_mod.encodeRecord(alloc, .{ .sequence = sequence, .changed_doc_keys = &.{"doc"}, .target_hints = &.{.dense_vector} });
            defer alloc.free(payload);
            for (hints) |hint| {
                const key = internal_keys.replayEntryKey(hint, sequence);
                try batch.put(&key, payload);
            }
        }
        try batch.commit();
    }
    const resources = @import("resource_manager.zig");
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = 128 * 1024 };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = 128 * 1024 } });
    defer manager.deinit(alloc);
    var bounded = resources.BudgetedAllocator.init(&manager, .lsm_read_working_set, alloc, 1);
    defer bounded.deinit();
    try store.truncateReplayUpTo(bounded.allocator(), cutoff);
    // A second pass has no retired debt. Both lanes preserve the next row.
    try store.truncateReplayUpTo(bounded.allocator(), cutoff);
    for (hints) |hint| {
        const retired = internal_keys.replayEntryKey(hint, cutoff);
        try std.testing.expectError(error.NotFound, store.get(alloc, &retired));
        const remaining = internal_keys.replayEntryKey(hint, cutoff + 1);
        const payload = try store.get(alloc, &remaining);
        defer alloc.free(payload);
        var decoded = try change_journal_mod.decodeRecord(alloc, payload);
        defer decoded.deinit();
        try std.testing.expectEqual(cutoff + 1, decoded.record.sequence);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
}

test "docstore indexes replay rows by hint and truncates them" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(alloc, path, .{});
    defer store.close();

    const embedding_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(embedding_artifact_key);
    const graph_artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "links", "doc:b");
    defer alloc.free(graph_artifact_key);
    const graph_asset_artifact_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(graph_asset_artifact_key);
    const graph_chunk_artifact_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "relation_chunks_v1", 0);
    defer alloc.free(graph_chunk_artifact_key);
    const record = change_journal_mod.Record{
        .sequence = 7,
        .changed_doc_keys = &.{"doc:a"},
        .deleted_doc_keys = &.{"doc:deleted"},
        .overwritten_doc_keys = &.{"doc:old"},
        .changed_artifact_keys = &.{ embedding_artifact_key, graph_artifact_key, graph_asset_artifact_key, graph_chunk_artifact_key },
        .target_hints = &.{ .dense_vector, .full_text, .graph },
    };
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);

    try store.putBatchWithReplay(null, &.{
        .{ .key = "doc:a", .value = "A" },
    }, &.{}, .{
        .sequence = 7,
        .payload = payload,
    });

    try std.testing.expect(try store.hasReplayEntries());
    try std.testing.expectEqual(DocStore.replay_index_available, store.replay_index_state.load(.monotonic));

    const all_entries = try store.iterateReplayFrom(alloc, 7);
    defer {
        for (all_entries) |*entry| entry.deinit(alloc);
        alloc.free(all_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), all_entries.len);
    try std.testing.expectEqualSlices(u8, payload, all_entries[0].payload);

    const dense_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .dense_vector);
    defer {
        for (dense_entries) |*entry| entry.deinit(alloc);
        alloc.free(dense_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), dense_entries.len);
    try std.testing.expectEqual(@as(u64, 7), dense_entries[0].sequence);
    var dense_record = try change_journal_mod.decodeRecord(alloc, dense_entries[0].payload);
    defer dense_record.deinit();
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.dense_vector, dense_record.record.target_hints[0]);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.deleted_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings(embedding_artifact_key, dense_record.record.changed_artifact_keys[0]);

    const full_text_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .full_text);
    defer {
        for (full_text_entries) |*entry| entry.deinit(alloc);
        alloc.free(full_text_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), full_text_entries.len);
    var full_text_record = try change_journal_mod.decodeRecord(alloc, full_text_entries[0].payload);
    defer full_text_record.deinit();
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.full_text, full_text_record.record.target_hints[0]);
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.deleted_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 0), full_text_record.record.changed_artifact_keys.len);

    const graph_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .graph);
    defer {
        for (graph_entries) |*entry| entry.deinit(alloc);
        alloc.free(graph_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), graph_entries.len);
    var graph_record = try change_journal_mod.decodeRecord(alloc, graph_entries[0].payload);
    defer graph_record.deinit();
    try std.testing.expectEqual(@as(usize, 1), graph_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.graph, graph_record.record.target_hints[0]);
    try std.testing.expectEqual(@as(usize, 0), graph_record.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), graph_record.record.deleted_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 0), graph_record.record.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 3), graph_record.record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings(graph_artifact_key, graph_record.record.changed_artifact_keys[0]);
    try std.testing.expectEqualStrings(graph_asset_artifact_key, graph_record.record.changed_artifact_keys[1]);
    try std.testing.expectEqualStrings(graph_chunk_artifact_key, graph_record.record.changed_artifact_keys[2]);

    const sparse_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .sparse_vector);
    defer {
        for (sparse_entries) |*entry| entry.deinit(alloc);
        alloc.free(sparse_entries);
    }
    try std.testing.expectEqual(@as(usize, 0), sparse_entries.len);

    try store.truncateReplayUpTo(alloc, 7);
    try std.testing.expect(try store.hasReplayEntries());
    try std.testing.expectEqual(DocStore.replay_index_available, store.replay_index_state.load(.monotonic));
    const after = try store.iterateReplayEntriesFromHint(alloc, 7, .dense_vector);
    defer {
        for (after) |*entry| entry.deinit(alloc);
        alloc.free(after);
    }
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "docstore relational bulk appends retain direct ingest and atomic dirty tokens" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 2,
    });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();

    {
        var batch = try store.beginWriteBatchWithOptions(.{ .mode = .bulk_ingest });
        errdefer batch.abort();
        var txn = batch.asTxn();
        try std.testing.expect(txn.unordered_bulk_append_puts);
        for (0..256) |i| {
            var id_buf: [16]u8 = undefined;
            const id = try std.fmt.bufPrint(&id_buf, "row:{d:0>4}", .{i});
            const key = try internal_keys.relationalRowKeyAlloc(alloc, id);
            defer alloc.free(key);
            try txn.appendPut(key, id);
        }
        try batch.commit();
    }
    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), stats.sorted_ingest_runs);
    try std.testing.expectEqual(@as(u64, 1), store.columnar_revision.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    for (0..256) |i| {
        var id_buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "row:{d:0>4}", .{i});
        const key = try internal_keys.relationalRowKeyAlloc(alloc, id);
        defer alloc.free(key);
        const value = try store.get(alloc, key);
        defer alloc.free(value);
        try std.testing.expectEqualStrings(id, value);
        const dirty = try internal_keys.relationalColumnarDirtyKeyAlloc(alloc, key);
        defer alloc.free(dirty);
        const token = try store.get(alloc, dirty);
        defer alloc.free(token);
        try std.testing.expectEqualSlices(u8, &internal_keys.relationalColumnarDirtyRecord(internal_keys.relationalColumnarMutationToken(1), id.len), token);
    }
    const key = try internal_keys.relationalRowKeyAlloc(alloc, "row:0000");
    defer alloc.free(key);
    const dirty = try internal_keys.relationalColumnarDirtyKeyAlloc(alloc, key);
    defer alloc.free(dirty);
    {
        var batch = try store.beginWriteBatchWithOptions(.{ .mode = .bulk_ingest });
        errdefer batch.abort();
        var txn = batch.asTxn();
        try txn.appendPut(key, "first");
        try txn.appendPut(key, "last");
        try batch.commit();
    }
    {
        var batch = try store.beginWriteBatchWithOptions(.{ .mode = .bulk_ingest });
        defer batch.abort();
        try batch.asTxn().appendPut(key, "aborted");
    }
    const value = try store.get(alloc, key);
    defer alloc.free(value);
    try std.testing.expectEqualStrings("last", value);
    try std.testing.expectEqual(@as(u64, 2), store.columnar_revision.load(.acquire));
    const token = try store.get(alloc, dirty);
    defer alloc.free(token);
    try std.testing.expectEqualSlices(u8, &internal_keys.relationalColumnarDirtyRecord(internal_keys.relationalColumnarMutationToken(2), 4), token);
    try store.put(key, "last");
    const rewritten = try store.get(alloc, dirty);
    defer alloc.free(rewritten);
    try std.testing.expectEqualSlices(u8, &internal_keys.relationalColumnarDirtyRecord(internal_keys.relationalColumnarMutationToken(3), 4), rewritten);
    try store.delete(key);
    const deleted = try store.get(alloc, dirty);
    defer alloc.free(deleted);
    try std.testing.expectEqualSlices(u8, &internal_keys.relationalColumnarDirtyRecord(internal_keys.relationalColumnarMutationToken(4), 0), deleted);
    var exhausted: [8]u8 = undefined;
    std.mem.writeInt(u64, &exhausted, std.math.maxInt(u64), .little);
    try store.put(internal_keys.relational_columnar_mutation_key, &exhausted);
    try std.testing.expectError(error.ColumnMutationVersionExhausted, store.put(key, "must not commit"));
    try std.testing.expectError(error.NotFound, store.get(alloc, key));
    const still_deleted = try store.get(alloc, dirty);
    defer alloc.free(still_deleted);
    try std.testing.expectEqualSlices(u8, deleted, still_deleted);
    try std.testing.expectEqual(@as(u64, 4), store.columnar_revision.load(.acquire));
}

test "docstore snapshot pages bound copies and preserve the source epoch" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.put("a", "alpha");
    try store.put("b", "beta");
    try store.put("c", "gamma");
    var txn = try store.beginReadTxn();
    defer txn.abort();
    try store.put("b", "new beta");
    try store.put("d", "new row");

    // One oversized first row must still make progress; the next row must
    // not be copied past the page's byte budget.
    var first = try store.scanReadTxnPage(alloc, &txn, "a", false, "z", 10, 1);
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), first.items.len);
    try std.testing.expect(!first.reached_end);
    var second = try store.scanReadTxnPage(alloc, &txn, first.items[0].key, true, "z", 1, 100);
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second.items.len);
    try std.testing.expectEqualStrings("beta", second.items[0].value);
    try std.testing.expect(!second.reached_end);
    var last = try store.scanReadTxnPage(alloc, &txn, second.items[0].key, true, "z", 10, 100);
    defer last.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), last.items.len);
    try std.testing.expectEqualStrings("c", last.items[0].key);
    try std.testing.expect(last.reached_end);
    try std.testing.expectError(error.InvalidArgument, store.scanReadTxnPage(alloc, &txn, "", false, "z", 0, 100));
}

test "docstore runtime point get does not clone mutable snapshot" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1024 });
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    try store.put("doc:a", "alpha");
    const before = backend.snapshotMaintenanceStats();
    const value = try store.get(alloc, "doc:a");
    defer alloc.free(value);
    try std.testing.expectEqualStrings("alpha", value);
    const before_copies = backend.snapshotReadStats().point_value_copies;
    {
        var probe = try store.beginProbeTxn();
        defer probe.abort();
        try std.testing.expect(try probe.contains("doc:a"));
        try std.testing.expect(!try probe.contains("doc:"));
        try std.testing.expect(!try probe.contains("doc:missing"));
        try store.delete("doc:a");
        try std.testing.expect(!try probe.contains("doc:a"));
    }
    try std.testing.expectEqual(before_copies, backend.snapshotReadStats().point_value_copies);
    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}

test "docstore consistent point batch owns values without cloning mutable state" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.putBatch(&.{
        .{ .key = "counter:a", .value = "old-a" },
        .{ .key = "counter:b", .value = "old-b" },
    }, &.{});
    const before = backend.snapshotMaintenanceStats();
    var values: [3]?[]const u8 = undefined;
    var lease = try store.readManyConsistent(&.{ "counter:a", "counter:b", "counter:missing" }, &values);
    defer lease.abort();
    try store.putBatch(&.{
        .{ .key = "counter:a", .value = "new-a" },
        .{ .key = "counter:missing", .value = "new-c" },
    }, &.{"counter:b"});
    try std.testing.expectEqualStrings("old-a", values[0].?);
    try std.testing.expectEqualStrings("old-b", values[1].?);
    try std.testing.expect(values[2] == null);
    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}

test "docstore runtime lsm hint replay iteration avoids ordinary read snapshots" {
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

    const record = change_journal_mod.Record{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    };
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);

    try store.putBatchWithReplay(null, &.{.{ .key = "doc:a", .value = "{}" }}, &.{}, .{
        .sequence = 1,
        .payload = payload,
    });

    const Context = struct {
        seen: usize = 0,
        fn handle(self: *@This(), sequence: u64, entry_payload: []const u8) !void {
            try std.testing.expectEqual(@as(u64, 1), sequence);
            try std.testing.expect(entry_payload.len > 0);
            self.seen += 1;
        }

        fn handleErased(ptr: *anyopaque, sequence: u64, entry_payload: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.handle(sequence, entry_payload);
        }
    };
    var ctx = Context{};
    try store.forEachReplayEntryFromHint(1, .full_text, &ctx, Context.handle);
    try std.testing.expectEqual(@as(usize, 1), ctx.seen);

    var erased_ctx = Context{};
    var replay_stats = ReplayIterationStats{};
    try store.forEachReplayFromMatchingHintMaskWithStats(
        1,
        change_journal_mod.singleHintMask(.full_text),
        &erased_ctx,
        Context.handleErased,
        &replay_stats,
    );
    try std.testing.expectEqual(@as(usize, 1), erased_ctx.seen);
    try std.testing.expectEqual(@as(usize, 1), replay_stats.scan_batches);
    try std.testing.expectEqual(@as(usize, 1), replay_stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 1), replay_stats.matched_entries);
    try std.testing.expectEqual(@as(usize, 0), replay_stats.hint_filter_skips);
    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.bound_read_txn)].calls);
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_snapshot_clone_by_reason[@backingInt(lsm_backend.MutableSnapshotReason.namespace_read_txn)].calls);
}

test "docstore runtime lsm persists replay rows across namespace reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
        });
        defer backend.close();

        const runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
        var store = try DocStore.openRuntime(alloc, runtime_store);
        defer store.close();

        try store.putBatchWithReplay(null, &.{.{ .key = "doc:a", .value = "{}" }}, &.{}, .{
            .sequence = 1,
            .payload = "replay:1",
        });
        try store.sync(true);

        const live_entries = try store.iterateReplayFrom(alloc, 1);
        defer {
            for (live_entries) |*entry| entry.deinit(alloc);
            alloc.free(live_entries);
        }
        try std.testing.expectEqual(@as(usize, 1), live_entries.len);
    }

    var reopened_backend = try lsm_backend.Backend.open(alloc, path, .{});
    defer reopened_backend.close();

    const reopened_runtime_store = try reopened_backend.runtimeStore(alloc, .{ .name = "docs" });
    var reopened = try DocStore.openRuntime(alloc, reopened_runtime_store);
    defer reopened.close();

    const entries = try reopened.iterateReplayFrom(alloc, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("replay:1", entries[0].payload);
}

test "graph incoming directory follows primary transaction commit and rollback" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    const target = "target\x00binary";
    const legacy = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "source", "facts", "R", target);
    defer alloc.free(legacy);
    const explicit = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "source", "facts", "R", target, "source", "id");
    defer alloc.free(explicit);
    const fact = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "fact", "facts", "R", target, "source", "fact");
    defer alloc.free(fact);
    const prefix = try internal_keys.graphIncomingPrefixAlloc(alloc, target);
    defer alloc.free(prefix);
    const explicit_ref = (try internal_keys.graphIncomingKeyAlloc(alloc, explicit)).?;
    defer alloc.free(explicit_ref);
    try std.testing.expect((try internal_keys.graphIncomingKeyAlloc(alloc, fact)) == null);
    try store.putBatch(&.{ .{ .key = legacy, .value = "legacy" }, .{ .key = explicit, .value = "explicit" }, .{ .key = fact, .value = "fact" } }, &.{});
    {
        const refs = try store.scanPrefix(alloc, prefix);
        defer DocStore.freeResults(alloc, refs);
        try std.testing.expectEqual(@as(usize, 2), refs.len);
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(explicit);
        try std.testing.expectError(error.NotFound, txn.get(explicit_ref));
    }
    {
        var batch = try store.beginWriteBatch();
        defer batch.abort();
        try batch.delete(legacy);
    }
    const refs = try store.scanPrefix(alloc, prefix);
    defer DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 2), refs.len);
    // Backfill an old store, then exercise maintenance after the ready marker.
    for (refs) |ref| try store.delete(ref.key);
    try store.delete(internal_keys.graph_incoming_ready_key);
    try store.ensureGraphIncomingDirectory();
    try store.ensureGraphIncomingDirectory();
    const recovered = try store.get(alloc, explicit_ref);
    defer alloc.free(recovered);
    try std.testing.expectEqualStrings(explicit, recovered);
    try store.delete(explicit);
    try std.testing.expectError(error.NotFound, store.get(alloc, explicit_ref));
    try store.put(explicit, "new");
    const renewed = try store.get(alloc, explicit_ref);
    defer alloc.free(renewed);
    try std.testing.expectEqualStrings(explicit, renewed);
}

test "graph incoming directory backfill resumes bounded pages" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    {
        var store = try DocStore.open(alloc, path, .{});
        defer store.close();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const temporary = arena.allocator();
        var writes = std.ArrayListUnmanaged(KVPair).empty;
        for (0..800) |i| {
            const owner = try std.fmt.allocPrint(temporary, "{d:0>8}", .{i});
            const key = try internal_keys.graphEdgeArtifactKeyAlloc(temporary, owner, "g", "R", "target");
            try writes.append(temporary, .{ .key = key, .value = "payload" });
        }
        try store.putBatch(writes.items, &.{});
        const ready = try store.get(alloc, internal_keys.graph_incoming_ready_key);
        defer alloc.free(ready);
        try std.testing.expectEqualStrings("1", ready);
        // New stores are ready before deletion. Remove derived metadata to
        // simulate an old store and verify one work quantum is bounded.
        const refs = try store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
        defer DocStore.freeResults(alloc, refs);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        for (refs) |ref| try deletes.append(temporary, ref.key);
        try deletes.append(temporary, internal_keys.graph_incoming_ready_key);
        try store.putBatch(&.{}, deletes.items);
        try std.testing.expect(!try store.backfillGraphIncomingDirectoryPage());
        const first_page = try store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
        defer DocStore.freeResults(alloc, first_page);
        try std.testing.expectEqual(@as(usize, 256), first_page.len);
        // Writers between pages must maintain entries behind the checkpoint,
        // and deletion ahead of it must not resurrect a stale directory entry.
        const late = try internal_keys.graphRelationshipArtifactKeyAlloc(temporary, "00000000", "g", "R", "target", "00000000", "late");
        try store.put(late, "late");
        try store.delete(writes.items[799].key);
    }
    {
        var store = try DocStore.open(alloc, path, .{});
        defer store.close();
        const checkpoint = try store.get(alloc, internal_keys.graph_incoming_cursor_key);
        defer alloc.free(checkpoint);
        var pages: usize = 0;
        while (true) {
            pages += 1;
            const complete = try store.backfillGraphIncomingDirectoryPage();
            try std.testing.expect(pages <= 3);
            if (complete) break;
        }
        try std.testing.expectError(error.NotFound, store.get(alloc, internal_keys.graph_incoming_cursor_key));
        const refs = try store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
        defer DocStore.freeResults(alloc, refs);
        try std.testing.expectEqual(@as(usize, 800), refs.len);
        const gone = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "00000799", "g", "R", "target");
        defer alloc.free(gone);
        const gone_ref = (try internal_keys.graphIncomingKeyAlloc(alloc, gone)).?;
        defer alloc.free(gone_ref);
        try std.testing.expectError(error.NotFound, store.get(alloc, gone_ref));
    }
}

test "graph relationship bulk ingestion preserves direct append and retirement" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1, .bulk_ingest_flush_threshold_multiplier = 2 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    var writes = std.ArrayListUnmanaged(KVPair).empty;
    for (0..512) |i| {
        const owner = try std.fmt.allocPrint(temporary, "{d:0>8}", .{i});
        const key = try internal_keys.graphRelationshipArtifactKeyAlloc(temporary, owner, "g", "R", "target", owner, "id");
        try writes.append(temporary, .{ .key = key, .value = "payload" });
    }
    {
        var batch = try store.beginWriteBatchWithOptions(.{ .mode = .bulk_ingest });
        errdefer batch.abort();
        var txn = batch.asTxn();
        for (writes.items) |write| try txn.appendPut(write.key, write.value);
        try batch.commit();
    }
    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), stats.sorted_ingest_runs);
    const refs = try store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
    defer DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 512), refs.len);
    const retired = try internal_keys.graphRetirementKeyAlloc(temporary, writes.items[0].key);
    {
        var batch = try store.beginWriteBatch();
        defer batch.abort();
        try batch.put(retired, "1");
    }
    const before = try store.get(alloc, writes.items[0].key);
    defer alloc.free(before);
    try std.testing.expectEqualStrings("payload", before);
    try store.put(retired, "1");
    try std.testing.expectError(error.NotFound, store.get(alloc, writes.items[0].key));
    {
        var batch = try store.beginWriteBatchWithOptions(.{ .mode = .bulk_ingest });
        errdefer batch.abort();
        var txn = batch.asTxn();
        try std.testing.expectError(error.Unsupported, txn.appendPut(writes.items[0].key, "revived"));
        try txn.put(writes.items[0].key, "revived");
        try txn.put(writes.items[1].key, "updated");
        try batch.commit();
    }
    try std.testing.expectError(error.NotFound, store.get(alloc, writes.items[0].key));
    const after = try store.get(alloc, writes.items[1].key);
    defer alloc.free(after);
    try std.testing.expectEqualStrings("updated", after);
    const runs_before = backend.snapshotWriteStats().sorted_ingest_runs;
    try store.putBatchWithReplayWithOptions(null, writes.items, &.{}, null, .{ .mode = .bulk_ingest });
    try std.testing.expectEqual(runs_before + 1, backend.snapshotWriteStats().sorted_ingest_runs);
    try std.testing.expectError(error.NotFound, store.get(alloc, writes.items[0].key));
    try std.testing.expect(try store.hasGraphRetirements());
    try store.delete(retired);
    try std.testing.expect(!try store.hasGraphRetirements());
    const runs_after = backend.snapshotWriteStats().sorted_ingest_runs;
    try store.putBatchWithReplayWithOptions(null, writes.items, &.{}, null, .{ .mode = .bulk_ingest });
    try std.testing.expectEqual(runs_after + 1, backend.snapshotWriteStats().sorted_ingest_runs);
    const revived = try store.get(alloc, writes.items[0].key);
    defer alloc.free(revived);
    try std.testing.expectEqualStrings("payload", revived);
}

test "graph incoming directory backfill resumes bounded pages with retirement accounting" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var markers = std.ArrayListUnmanaged(KVPair).empty;
    for (0..600) |i| {
        const owner = try std.fmt.allocPrint(scratch, "{d:0>8}", .{i});
        const artifact = try internal_keys.graphEdgeArtifactKeyAlloc(scratch, owner, "g", "R", "target");
        try markers.append(scratch, .{ .key = try internal_keys.graphRetirementKeyAlloc(scratch, artifact), .value = "1" });
    }
    try store.putBatch(markers.items, &.{});
    // Simulate a v1 store: primary markers and its conservative presence flag
    // survive, but v2 count/reference metadata does not exist yet.
    const refs = try store.scanPrefix(alloc, internal_keys.graph_retirement_ref_prefix);
    defer DocStore.freeResults(alloc, refs);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    for (refs) |ref| try deletes.append(scratch, ref.key);
    try deletes.appendSlice(scratch, &.{ internal_keys.graph_incoming_ready_key, internal_keys.graph_retirement_count_key });
    try store.putBatch(&.{}, deletes.items);
    try std.testing.expect(!try store.backfillGraphIncomingDirectoryPage());
    const checkpoint = try store.get(alloc, internal_keys.graph_incoming_cursor_key);
    defer alloc.free(checkpoint);
    // Existing refs are idempotent; writes behind and deletes ahead of the
    // checkpoint compose with migration without double counting.
    try store.put(markers.items[0].key, "1");
    try store.delete(markers.items[599].key);
    const late_artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(scratch, "00000000", "g", "R", "target", "00000000", "late");
    const late_marker = try internal_keys.graphRetirementKeyAlloc(scratch, late_artifact);
    try store.put(late_marker, "1");
    try store.ensureGraphIncomingDirectory();
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, 600), try graphRetirementCount(&txn));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(markers.items[0].key);
        try std.testing.expectEqual(@as(u64, 599), try graphRetirementCount(&txn));
    }
    try std.testing.expect(try store.hasGraphRetirements());
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, 600), try graphRetirementCount(&txn));
    }
    var final_deletes = std.ArrayListUnmanaged([]const u8).empty;
    for (markers.items) |marker| try final_deletes.append(scratch, marker.key);
    try final_deletes.append(scratch, late_marker);
    try store.putBatch(&.{}, final_deletes.items);
    try std.testing.expect(!try store.hasGraphRetirements());
}

test "graph incoming directory backfill resumes bounded pages after physical reset" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var markers = std.ArrayListUnmanaged(KVPair).empty;
    for (0..600) |i| {
        const owner = try std.fmt.allocPrint(scratch, "{d:0>8}", .{i});
        const artifact = try internal_keys.graphEdgeArtifactKeyAlloc(scratch, owner, "g", "R", "target");
        try markers.append(scratch, .{ .key = try internal_keys.graphRetirementKeyAlloc(scratch, artifact), .value = "1" });
    }
    try store.putBatch(markers.items, &.{});
    // Seed stale directories as a physical parent rewrite would retain them.
    const stale = try internal_keys.graphEdgeArtifactKeyAlloc(scratch, "gone", "g", "R", "target");
    const incoming = (try internal_keys.graphIncomingKeyAlloc(scratch, stale)).?;
    try store.put(incoming, stale);
    try store.invalidateGraphDirectories();
    // Model the crash window after physical pruning, which bypasses DocStore
    // delete hooks and leaves references to records that no longer exist.
    {
        var raw = try store.runtime_store.beginWrite();
        errdefer raw.abort();
        try raw.delete(markers.items[599].key);
        try raw.commit();
    }
    try store.delete(internal_keys.graph_retirement_present_key);
    // Missing presence metadata must never bypass a primary retirement.
    const artifact = try internal_keys.graphRetirementArtifactKeyAlloc(scratch, markers.items[0].key);
    try store.put(artifact, "revived");
    try std.testing.expectError(error.NotFound, store.get(alloc, artifact));
    try std.testing.expect(!try store.backfillGraphIncomingDirectoryPage());
    try std.testing.expect(!try store.backfillGraphIncomingDirectoryPage());
    // Resume with a new store handle after interrupting reference cleanup.
    var resumed = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer resumed.close();
    try resumed.ensureGraphIncomingDirectory();
    try std.testing.expectError(error.NotFound, resumed.get(alloc, incoming));
    try std.testing.expectError(error.NotFound, resumed.get(alloc, internal_keys.graph_directory_reset_key));
    var txn = try resumed.beginReadTxn();
    defer txn.abort();
    try std.testing.expectEqual(@as(u64, 599), try graphRetirementCount(&txn));
}

test "graph endpoint cleanup byte admission preserves independent facts" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var long_source = try scratch.alloc(u8, 16000);
    @memset(long_source, 'x');
    for (0..30) |i| {
        long_source[0] = @intCast('A' + i);
        const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(scratch, long_source, "g", "R", "hub", long_source, "");
        try store.put(artifact, "edge");
    }
    const fact = try internal_keys.graphRelationshipArtifactKeyAlloc(scratch, "fact", "g", "R", "hub", "entity", "one");
    try store.put(fact, "fact");
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(scratch, "hub");
    try store.put(job, "hub");
    var pages: usize = 0;
    var inspected: usize = 0;
    while (try store.prepareGraphEndpointCleanupPage(alloc)) |owned| {
        var page = owned;
        defer page.deinit();
        try std.testing.expect(page.inspected <= 256);
        try std.testing.expect(page.bytes <= 256 * 1024);
        inspected += page.inspected;
        try store.putBatch(page.writes, page.deletes);
        pages += 1;
    }
    try std.testing.expect(pages > 1);
    try std.testing.expectEqual(@as(usize, 30), inspected);
    const retained = try store.get(alloc, fact);
    defer alloc.free(retained);
    try std.testing.expectEqualStrings("fact", retained);
    try std.testing.expect(!try store.hasGraphEndpointCleanup());
}

test "graph endpoint cleanup byte admission releases partial page allocations" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    for ([_][]const u8{ "a", "b" }) |source| {
        const edge = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, source, "g", "R", "hub");
        defer alloc.free(edge);
        try store.put(edge, "edge");
    }
    try store.ensureGraphIncomingDirectory();
    for ([_][]const u8{ "hub", "empty" }) |endpoint| {
        const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, endpoint);
        defer alloc.free(job);
        try store.put(job, endpoint);
    }
    const Check = struct {
        fn run(page_alloc: Allocator, input: *DocStore) !void {
            var page = (try input.prepareGraphEndpointCleanupPage(page_alloc)).?;
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 2), page.writes.len);
            try std.testing.expectEqual(@as(usize, 2), page.deletes.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Check.run, .{&store});
}

test "graph relationship bulk ingestion preserves direct append and retirement during migration" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1, .bulk_ingest_flush_threshold_multiplier = 2 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(KVPair, 600);
    for (writes, 0..) |*write, i| {
        const owner = try std.fmt.allocPrint(scratch, "owner:{d:0>4}", .{i});
        write.* = .{ .key = try internal_keys.graphRelationshipArtifactKeyAlloc(scratch, owner, "g", "R", "hub", owner, "id"), .value = "payload" };
    }
    try store.putBatch(writes, &.{});
    const retired = try internal_keys.graphRetirementKeyAlloc(scratch, writes[0].key);
    try store.put(retired, "1");
    try store.delete(internal_keys.graph_incoming_ready_key);
    // Incomplete directory/count coverage cannot cause retired bulk rows to
    // reappear, and unrelated rows still use the sorted append fast path.
    const before = backend.snapshotWriteStats().sorted_ingest_runs;
    try store.putBatchWithReplayWithOptions(null, writes, &.{}, null, .{ .mode = .bulk_ingest });
    try std.testing.expectEqual(before + 1, backend.snapshotWriteStats().sorted_ingest_runs);
    try std.testing.expectError(error.NotFound, store.get(alloc, writes[0].key));
    const live = try store.get(alloc, writes[1].key);
    defer alloc.free(live);
    try std.testing.expectEqualStrings("payload", live);
    try std.testing.expect(!try store.graphIncomingDirectoryReady());
    try std.testing.expectError(error.NotFound, store.get(alloc, internal_keys.graph_incoming_cursor_key));
    // Only bounded maintenance creates a migration checkpoint.
    try std.testing.expect(!try store.backfillGraphIncomingDirectoryPage());
    try std.testing.expect(!try store.graphIncomingDirectoryReady());
}

test "graph relationship bulk ingestion preserves direct append and retirement with initialized directory" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1, .bulk_ingest_flush_threshold_multiplier = 2 });
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    // Populate the directory before the bulk transaction: initialization must
    // not accidentally hide unordered-overlay reads behind a mutable write.
    const initial = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "seed", "g", "R", "target", "seed", "seed");
    defer alloc.free(initial);
    try store.put(initial, "seed");
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(KVPair, 4096);
    for (writes, 0..) |*write, i| {
        const owner = try std.fmt.allocPrint(scratch, "{d:0>8}", .{i});
        write.* = .{ .key = try internal_keys.graphRelationshipArtifactKeyAlloc(scratch, owner, "g", "R", "target", owner, "id"), .value = "payload" };
    }
    const before = backend.snapshotWriteStats().sorted_ingest_runs;
    try store.putBatchWithReplayWithOptions(null, writes, &.{}, null, .{ .mode = .bulk_ingest });
    try std.testing.expectEqual(before + 1, backend.snapshotWriteStats().sorted_ingest_runs);
    const refs = try store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
    defer DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(writes.len + 1, refs.len);
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(scratch, "target");
    try store.put(job, "target");
    try std.testing.expectError(error.IntegrityTopologyBusy, store.putBatchWithReplayWithOptions(null, writes, &.{}, null, .{ .mode = .bulk_ingest }));
}

test "graph endpoint cleanup byte admission has bounded allocation growth for empty jobs" {
    var previous_bytes: usize = 0;
    for ([_]usize{ 64, 128, 256 }) |size| {
        var counter = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        const alloc = counter.allocator();
        var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1_000_000 });
        defer backend.close();
        var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        const initial = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "seed", "g", "R", "target", "seed", "seed");
        defer alloc.free(initial);
        try store.put(initial, "seed");
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(KVPair, size);
        for (writes, 0..) |*write, i| {
            const endpoint = try std.fmt.allocPrint(scratch, "empty:{d}", .{i});
            write.* = .{ .key = try internal_keys.graphEndpointCleanupKeyAlloc(scratch, endpoint), .value = endpoint };
        }
        const before = counter.allocated_bytes;
        try store.putBatch(writes, &.{});
        const bytes = counter.allocated_bytes - before;
        // Allow tree/container growth, but reject repeated overlay copies
        // (the old cursor path grew by more than 3.5x on every doubling).
        if (previous_bytes != 0) try std.testing.expect(bytes < previous_bytes * 3);
        previous_bytes = bytes;
        try std.testing.expect(try store.hasGraphEndpointCleanup());
        try std.testing.expect(!try store.graphEndpointCleanupBlocksReads());
    }
}

test "graph endpoint cleanup byte admission observes pending incoming additions and deletions" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const edge = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "source", "g", "R", "target", "source", "id");
    defer alloc.free(edge);
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "target");
    defer alloc.free(job);
    try store.put(edge, "edge");
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(edge);
        try txn.put(job, "target");
        try txn.commit();
    }
    try std.testing.expect(!try store.graphEndpointCleanupBlocksReads());
    try store.delete(job);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(edge, "edge");
        try txn.put(job, "target");
        try txn.commit();
    }
    try std.testing.expect(try store.graphEndpointCleanupBlocksReads());
}

test "graph qualified cleanup isolates foreign targets and foreign-source facts" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    try store.put(internal_keys.graph_owning_table_key, "facts");
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const codec = @import("db/enrichment/artifact_codec.zig");
    const metadata = [_][]const u8{ "{}", "{\"target_table\":\"facts\"}", "{\"target_table\":\"people\"}", "{\"source_table\":\"people\",\"target_table\":\"facts\"}" };
    var edges: [4][]const u8 = undefined;
    var values: [4][]const u8 = undefined;
    for (metadata, 0..) |raw, i| {
        edges[i] = try internal_keys.graphRelationshipArtifactKeyAlloc(a, "source", "g", "R", "shared", "source", try std.fmt.allocPrint(a, "{d}", .{i}));
        values[i] = try codec.encodeGraphEdgeAlloc(a, null, 1, 1, 0, 0, raw);
        try store.put(edges[i], values[i]);
    }
    try store.ensureGraphIncomingDirectory();
    const prefix = try internal_keys.graphIncomingPrefixAlloc(a, "shared");
    const refs = try store.scanPrefix(alloc, prefix);
    defer DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 2), refs.len);
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(a, "shared");
    try store.put(job, "shared");
    try std.testing.expectError(error.IntegrityTopologyBusy, store.put(edges[0], values[0]));
    try std.testing.expectError(error.IntegrityTopologyBusy, store.put(edges[1], values[1]));
    try store.put(edges[2], values[2]);
    try store.put(edges[3], values[3]);
    while (try store.prepareGraphEndpointCleanupPage(alloc)) |owned| {
        var page = owned;
        defer page.deinit();
        try store.putBatch(page.writes, page.deletes);
    }
    for (edges, 0..) |edge, i| try std.testing.expectEqual(i < 2, try store.graphRelationshipRetired(edge));
    for (edges[2..]) |edge| {
        const value = try store.get(alloc, edge);
        defer alloc.free(value);
    }
}

test "graph owner revival pages bound history and inputs and retain newer deletions" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const contract = @import("graph_cleanup_contract.zig");
    var rows = std.ArrayListUnmanaged(KVPair).empty;
    const input_value = try a.alloc(u8, 2048);
    @memset(input_value, 'x');
    for (0..600) |i| {
        const id = try std.fmt.allocPrint(a, "{d:0>4}", .{i});
        const edge = try internal_keys.graphRelationshipArtifactKeyAlloc(a, "owner", "g", "R", "target", "owner", id);
        try rows.append(a, .{ .key = try internal_keys.graphRetirementKeyAlloc(a, edge), .value = "1" });
        try rows.append(a, .{ .key = try internal_keys.artifactNamedPrefixAlloc(a, "owner", "asset", id), .value = input_value });
    }
    try store.putBatch(rows.items, &.{});
    const new_edge = try internal_keys.graphRelationshipArtifactKeyAlloc(a, "owner", "g", "R", "target", "owner", "new");
    const new_marker = try internal_keys.graphRetirementKeyAlloc(a, new_edge);
    const stamp = contract.retirementValue(51);
    try store.put(new_marker, &stamp);
    const job_key = try contract.ownerJobKeyAlloc(a, "owner");
    try store.put(job_key, try contract.encodeOwnerJobAlloc(a, .{ .owner = "owner", .generation = 50 }));
    try std.testing.expect(try store.graphEndpointCleanupBlocksReads());
    var pages: usize = 0;
    var inputs: usize = 0;
    while (try store.prepareGraphEndpointCleanupPage(alloc)) |owned| {
        var page = owned;
        defer page.deinit();
        try std.testing.expect(page.inspected <= 255);
        try std.testing.expect(page.bytes <= 256 * 1024);
        try std.testing.expect(page.deletes.len + page.replay_writes.len <= 256);
        for (page.replay_writes) |row| if (contract.isReplayInput(row.key)) {
            inputs += 1;
        };
        try store.putBatch(page.replay_writes, page.deletes);
        pages += 1;
    }
    try std.testing.expect(pages > 5);
    try std.testing.expectEqual(@as(usize, 600), inputs);
    const prefix = try internal_keys.graphRetirementPrefixAlloc(a, "owner");
    const remaining = try store.scanPrefixKeysPage(alloc, prefix, null, 10);
    defer {
        for (remaining) |key| alloc.free(key);
        alloc.free(remaining);
    }
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualStrings(new_marker, remaining[0]);
    try std.testing.expect(!try store.graphEndpointCleanupBlocksReads());
}

test "graph owner revival planning releases every partial allocation" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    try store.ensureGraphIncomingDirectory();
    const contract = @import("graph_cleanup_contract.zig");
    const key = try contract.ownerJobKeyAlloc(alloc, "owner");
    defer alloc.free(key);
    const value = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "owner", .generation = 1, .phase = .inputs });
    defer alloc.free(value);
    try store.put(key, value);
    const input_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "owner", "asset", "a");
    defer alloc.free(input_key);
    try store.put(input_key, "input");
    const Case = struct {
        fn plan(a: Allocator, owner: *DocStore) !void {
            var page = (try owner.prepareGraphEndpointCleanupPage(a)).?;
            defer page.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Case.plan, .{&store});
}

test "graph qualified directory upgrade removes stale foreign membership and preserves revived artifacts" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    const codec = @import("db/enrichment/artifact_codec.zig");
    const contract = @import("graph_cleanup_contract.zig");
    const foreign = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "a", "g", "R", "b", "a", "foreign");
    defer alloc.free(foreign);
    const local = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "a", "g", "R", "b", "a", "local");
    defer alloc.free(local);
    const foreign_value = try codec.encodeGraphEdgeAlloc(alloc, null, 1, 1, 0, 0, "{\"target_table\":\"people\"}");
    defer alloc.free(foreign_value);
    const local_value = try codec.encodeGraphEdgeAlloc(alloc, null, 1, 1, 0, 0, "{}");
    defer alloc.free(local_value);
    const retired = try internal_keys.graphRetirementKeyAlloc(alloc, local);
    defer alloc.free(retired);
    try store.put(retired, "1");
    const job_key = try contract.ownerJobKeyAlloc(alloc, "a");
    defer alloc.free(job_key);
    const job_value = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "a", .generation = 2 });
    defer alloc.free(job_value);
    try store.put(job_key, job_value);
    try store.put(local, local_value);
    try store.put(foreign, foreign_value);
    const stale = (try internal_keys.graphIncomingKeyAlloc(alloc, foreign)).?;
    defer alloc.free(stale);
    try store.put(stale, foreign);
    try store.put(internal_keys.graph_incoming_legacy_ready_key, "1");
    try store.delete(internal_keys.graph_incoming_ready_key);
    try store.ensureGraphIncomingDirectory();
    const refs = try store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
    defer DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 1), refs.len);
    try std.testing.expectEqualSlices(u8, local, refs[0].value);
    const revived = try store.get(alloc, local);
    defer alloc.free(revived);
    try std.testing.expectEqualSlices(u8, local_value, revived);
    try std.testing.expect(!try store.graphRelationshipRetired(local));
}

test "graph owner revival skips projected outputs and unrelated directory migration" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const rows = try a.alloc(KVPair, 600);
    for (rows, 0..) |*row, i| row.* = .{ .key = try internal_keys.graphRelationshipArtifactKeyAlloc(a, "owner", "g", "R", "b", "owner", try std.fmt.allocPrint(a, "{d}", .{i})), .value = "payload" };
    try store.putBatch(rows, &.{});
    try store.delete(internal_keys.graph_incoming_ready_key);
    const contract = @import("graph_cleanup_contract.zig");
    const job = try contract.ownerJobKeyAlloc(a, "owner");
    try store.put(job, try contract.encodeOwnerJobAlloc(a, .{ .owner = "owner", .generation = 2, .phase = .inputs }));
    var page = (try store.prepareGraphEndpointCleanupPage(alloc)).?;
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 0), page.inspected);
    try std.testing.expectEqual(@as(usize, 1), page.replay_writes.len);
    try std.testing.expectEqual(contract.OwnerPhase.chunks, (try contract.decodeOwnerJob(page.replay_writes[0].key, page.replay_writes[0].value)).phase);
    try std.testing.expect(!try store.graphIncomingDirectoryReady());
    try std.testing.expectError(error.NotFound, store.get(alloc, internal_keys.graph_incoming_cursor_key));
}

test "graph qualified bulk rewrites remove old local membership and admit fresh foreign rows" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    const codec = @import("db/enrichment/artifact_codec.zig");
    const edge = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "a", "g", "R", "b", "a", "id");
    defer alloc.free(edge);
    const local = try codec.encodeGraphEdgeAlloc(alloc, null, 1, 1, 0, 0, "{}");
    defer alloc.free(local);
    const foreign = try codec.encodeGraphEdgeAlloc(alloc, null, 1, 1, 0, 0, "{\"target_table\":\"people\"}");
    defer alloc.free(foreign);
    try store.put(edge, local);
    try store.putBatchWithReplayWithOptions(null, &.{.{ .key = edge, .value = foreign }}, &.{}, null, .{ .mode = .bulk_ingest });
    const refs = try store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
    defer DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 0), refs.len);
    const fresh = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "a", "g", "R", "b", "a", "fresh");
    defer alloc.free(fresh);
    try store.putBatchWithReplayWithOptions(null, &.{.{ .key = fresh, .value = foreign }}, &.{}, null, .{ .mode = .bulk_ingest });
    for ([_][]const u8{ edge, fresh }) |key| {
        const actual = try store.get(alloc, key);
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, foreign, actual);
    }
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "b");
    defer alloc.free(job);
    try store.put(job, "b");
    var page = (try store.prepareGraphEndpointCleanupPage(alloc)).?;
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 0), page.writes.len);
}

test "graph endpoint cleanup bulk retirement mask releases ownership on decoding failure" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const edge = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "source", "g", "R", "target", "source", "id");
    defer alloc.free(edge);
    const marker = try internal_keys.graphRetirementKeyAlloc(alloc, edge);
    defer alloc.free(marker);
    // Bypass normal admission to simulate a damaged persisted stamp. The same
    // ownership path must unwind allocation and storage errors during decoding.
    {
        var raw = try backend.beginBatchWithOptions(.{});
        defer raw.abort();
        try raw.put(.{}, marker, "invalid");
        try raw.commit();
    }
    var batch = try store.beginWriteBatch();
    defer batch.abort();
    try std.testing.expectError(error.InvalidGraphRetirement, DocStore.graphBulkRetirementMask(alloc, batch.asTxn(), &.{.{ .key = edge, .value = "edge" }}));
}

test "public graph metric borrowed wire content authority shares txn batch append abort and reopen boundaries" {
    const a = std.testing.allocator;
    const revision = @import("document_mutation_revision.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const key = try internal_keys.documentKeyAlloc(a, "doc");
    defer a.free(key);
    var expected: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("last", &expected, .{});
    for (0..2) |pass| {
        var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        if (pass == 0) {
            try store.put(key, "first");
            var pinned = try store.beginReadTxn();
            defer pinned.abort();
            const before = (try revision.loadContent(&pinned, key)).?;
            {
                var writer = try store.beginWriteTxn();
                defer writer.abort();
                try writer.put(key, "aborted");
            }
            {
                var batch = try store.beginWriteBatch();
                errdefer batch.abort();
                try batch.asTxn().appendPut(key, "last");
                try batch.commit();
            }
            try std.testing.expectEqualSlices(u8, &before.digest, &(try revision.loadContent(&pinned, key)).?.digest);
        }
        var current = try store.beginReadTxn();
        defer current.abort();
        try std.testing.expectEqualSlices(u8, &expected, &(try revision.loadContent(&current, key)).?.digest);
        try std.testing.expectEqualStrings("last", try current.get(key));
        if (pass == 1) {
            try store.delete(key);
            var deleted = try store.beginReadTxn();
            defer deleted.abort();
            try std.testing.expect((try revision.loadContent(&deleted, key)) == null);
            try std.testing.expectError(error.NotFound, deleted.get(key));
        }
    }
}

test "logical source certificates invalidate transaction and batch mutations including delete recreate" {
    const a = std.testing.allocator;
    const revision = @import("document_mutation_revision.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const x = try internal_keys.documentKeyAlloc(a, "x");
    defer a.free(x);
    const y = try internal_keys.documentKeyAlloc(a, "y");
    defer a.free(y);
    const key = revision.keyForPhysical(x).?;
    const identity = key[key.len - 32 ..].*;
    var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 1024 * 1024 });
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try store.put(x, "first");
    var captured = try store.beginReadTxn();
    defer captured.abort();
    const proof = (try revision.loadContent(&captured, x)).?;
    const initial = store.source_certificates.capture();
    store.source_certificates.put(a, initial, identity, .{ .content = proof, .timestamp = 0 });
    try store.put(y, "unrelated");
    try std.testing.expect(store.source_certificates.get(store.source_certificates.capture(), identity) != null);
    {
        var aborted = try store.beginWriteTxn();
        defer aborted.abort();
        try aborted.put(x, "aborted");
    }
    try std.testing.expect(store.source_certificates.get(store.source_certificates.capture(), identity) != null);
    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        try batch.asTxn().appendPut(x, "second");
        try batch.commit();
    }
    try std.testing.expect(store.source_certificates.get(store.source_certificates.capture(), identity) == null);
    store.source_certificates.put(a, initial, identity, .{ .content = proof, .timestamp = 0 });
    try std.testing.expect(store.source_certificates.get(store.source_certificates.capture(), identity) == null);
    {
        var current = try store.beginReadTxn();
        defer current.abort();
        store.source_certificates.put(a, store.source_certificates.capture(), identity, .{ .content = (try revision.loadContent(&current, x)).?, .timestamp = 0 });
    }
    try store.delete(x);
    try store.put(x, "third");
    try std.testing.expect(store.source_certificates.get(store.source_certificates.capture(), identity) == null);
    try std.testing.expectEqualStrings("first", try captured.get(x));
    try store.beginBulkIngestSession();
    try store.beginBulkIngestSession();
    try std.testing.expect(store.source_certificates.capture() == null);
    store.abortBulkIngestSession();
    try std.testing.expect(store.source_certificates.capture() == null);
    try store.finishBulkIngestSessionWithOptions(.{});
    try std.testing.expect(store.source_certificates.capture() != null);
}

test "document payload automatic reuse retains snapshots reads and bounded physical writes" {
    const a = std.testing.allocator;
    var backend = lsm_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const schema = @import("schema.zig");
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const key = try internal_keys.documentKeyAlloc(a, "large");
    defer a.free(key);
    const payload = try a.alloc(u8, 256 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{payload});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{payload});
    defer a.free(second);
    try store.put(key, first);
    var old = try store.beginReadTxn();
    defer old.abort();
    try std.testing.expect(!document_payload.isManifest(try old.getPhysical(key)));
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try write.put(key, second);
        try std.testing.expectEqual(payload.len + 2, write.document_revisions.document_payload_stats.blob_bytes_written);
        try write.commit();
    }
    var migrated = try store.beginReadTxn();
    defer migrated.abort();
    try std.testing.expect(document_payload.isManifest(try migrated.getPhysical(key)));
    try std.testing.expectEqualStrings(first, try old.get(key));
    try std.testing.expectEqualStrings(second, try migrated.get(key));
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try write.put(key, first);
        try std.testing.expectEqual(@as(usize, 0), write.document_revisions.document_payload_stats.blob_bytes_written);
        try std.testing.expectEqual(payload.len + 2, write.document_revisions.document_payload_stats.blob_bytes_reused);
        try std.testing.expect(write.document_revisions.document_payload_stats.manifest_bytes < 512);
        try write.commit();
    }
    try std.testing.expectEqualStrings(second, try migrated.get(key));
    {
        var current = try store.beginReadTxn();
        defer current.abort();
        var values: [1]?[]const u8 = undefined;
        try current.getManySorted(&.{key}, &values);
        try std.testing.expectEqualStrings(first, values[0].?);
        var scope = try current.openReadScope(a);
        defer scope.close();
        try std.testing.expectEqualStrings(first, try scope.get(key));
        var cursor = try current.openCursor();
        defer cursor.close();
        try std.testing.expectEqualStrings(first, (try cursor.seekAtOrAfter(key)).?.value);
    }
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try write.patchMetadata(key, &.{.{ .name = "metadata", .value_json = "3" }});
        // A second patch observes the first publication in this transaction.
        try write.patchMetadata(key, &.{.{ .name = "added", .value_json = "true" }});
        try std.testing.expectEqual(@as(usize, 0), write.document_revisions.document_payload_stats.blob_bytes_written);
        try std.testing.expect(write.document_revisions.document_payload_stats.manifest_bytes < 1024);
        try write.commit();
    }
    {
        var current = try store.beginReadTxn();
        defer current.abort();
        const actual = try current.get(key);
        const expected = try std.fmt.allocPrint(a, "{{\"added\":true,\"body\":\"{s}\",\"metadata\":3}}", .{payload});
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, actual);
        const proof = (try revisionForPayloadTest(&current, key)).?;
        const physical = try a.dupe(u8, try current.getPhysical(key));
        defer a.free(physical);
        const parsed = try document_payload.Manifest.decode(a, physical);
        defer a.free(parsed.parts);
        try std.testing.expect(parsed.rooted);
        try std.testing.expectEqualSlices(u8, &proof.digest, &parsed.logical_digest);
        const revision_key = @import("document_mutation_revision.zig").keyForPhysical(key).?;
        const paths = try a.dupe(u8, try current.getPhysical(&@import("document_projection.zig").keyForRevision(revision_key)));
        defer a.free(paths);
        const directory = (try @import("document_projection.zig").Directory.open(a, paths, proof)).?;
        defer a.free(directory.fields);
        const field = directory.find("metadata").?;
        var scope = try current.openReadScope(a);
        defer scope.close();
        var values: [1][]const u8 = undefined;
        try document_payload.readSelected(a, &scope, key, proof, &.{field}, &values);
        defer a.free(values[0]);
        try std.testing.expectEqualStrings("3", values[0]);
    }
    // Disposable proof loss must rebuild the tagged manifest commitment,
    // rather than installing SHA256(reconstructed JSON) with an inline hint.
    {
        const revisions = @import("document_mutation_revision.zig");
        try store.delete(&revisions.contentKey(revisions.keyForPhysical(key).?));
        var page = (try revisions.BackfillPage.prepare(a, &store, true)).?;
        defer page.deinit();
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try std.testing.expectEqual(@as(usize, 1), try page.stage(&write));
        try write.commit();
        var read = try store.beginReadTxn();
        defer read.abort();
        const rebuilt = (try revisions.loadContent(&read, key)).?;
        try std.testing.expect(rebuilt.manifest);
        try std.testing.expectEqualSlices(u8, &document_payload.publishedCommitment(try read.getPhysical(key)), &rebuilt.digest);
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        try std.testing.expectError(error.Unsupported, write.patchMetadata(key, &.{.{ .name = "body", .value_json = "\"replacement\"" }}));
        try write.patchMetadata(key, &.{.{ .name = "metadata", .value_json = "4" }});
        // Abort must leave both the manifest and all payload references intact.
    }
    try store.delete(key);
    try std.testing.expectError(error.NotFound, store.get(a, key));
    try std.testing.expectEqualStrings(second, try migrated.get(key));
}

fn revisionForPayloadTest(txn: anytype, key: []const u8) !?@import("document_mutation_revision.zig").Content {
    return @import("document_mutation_revision.zig").loadContent(txn, key);
}

test "document payload persistent metadata patches bound WAL writes and survive reopen" {
    const a = std.testing.allocator;
    const schema = @import("schema.zig");
    const revision = @import("document_mutation_revision.zig");
    const key = try internal_keys.documentKeyAlloc(a, "large");
    defer a.free(key);
    for ([_]usize{ 64 * 1024, 1024 * 1024 }) |length| {
        const payload = try a.alloc(u8, length);
        defer a.free(payload);
        @memset(payload, 'x');
        const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{payload});
        defer a.free(first);
        const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{payload});
        defer a.free(second);
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        for (0..2) |pass| {
            var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 16 * 1024 * 1024 });
            defer backend.close();
            var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
            defer store.close();
            store.document_payload_prototype = true;
            if (pass == 0) {
                const encoded = try schema.serializeSchema(a, .{});
                defer a.free(encoded);
                // ASCH25 and ASCH26 have the same schema fields. Seed a legacy
                // schema through the physical backend, as an old binary would.
                std.mem.writeInt(u32, encoded[4..8], 25, .little);
                try store.put(schema.schema_key, encoded);
                {
                    var untouched = try store.beginReadTxn();
                    defer untouched.abort();
                    try std.testing.expectEqual(@as(u32, 25), std.mem.readInt(u32, (try untouched.get(schema.schema_key))[4..8], .little));
                }
                try store.put(key, first);
                const read_rounds = 16;
                const inline_read_start = @import("antfly_platform").time.monotonicNs();
                for (0..read_rounds) |_| {
                    const value = try store.get(a, key);
                    defer a.free(value);
                    try std.testing.expectEqual(first.len, value.len);
                }
                const inline_read_ns = @import("antfly_platform").time.monotonicNs() - inline_read_start;
                try store.put(key, second);
                const rounds = 8;
                const before_put = backend.snapshotWriteStats();
                const put_start = @import("antfly_platform").time.monotonicNs();
                for (0..rounds) |i| try store.put(key, if (i % 2 == 0) first else second);
                const put_ns = @import("antfly_platform").time.monotonicNs() - put_start;
                const before_patch = backend.snapshotWriteStats();
                const patch_start = @import("antfly_platform").time.monotonicNs();
                for (0..rounds) |i| {
                    var write = try store.beginWriteTxn();
                    errdefer write.abort();
                    try write.patchMetadata(key, &.{.{ .name = "metadata", .value_json = if (i % 2 == 0) "1" else "2" }});
                    try std.testing.expectEqual(@as(usize, 0), write.document_revisions.document_payload_stats.blob_bytes_written);
                    try write.commit();
                }
                const patch_ns = @import("antfly_platform").time.monotonicNs() - patch_start;
                const after_patch = backend.snapshotWriteStats();
                const wal_patch = after_patch.wal_append_bytes - before_patch.wal_append_bytes;
                try std.testing.expect(wal_patch > 0 and wal_patch < 8192 * rounds);
                std.debug.print("DOCUMENT_PATCH_STORAGE payload_bytes={d} rounds={d} full_put_ns={d} patch_ns={d} full_put_wal_bytes={d} patch_wal_bytes={d}\n", .{ length, rounds, put_ns / rounds, patch_ns / rounds, (before_patch.wal_append_bytes - before_put.wal_append_bytes) / rounds, wal_patch / rounds });
                const manifest_read_start = @import("antfly_platform").time.monotonicNs();
                for (0..read_rounds) |_| {
                    const value = try store.get(a, key);
                    defer a.free(value);
                    try std.testing.expectEqual(second.len, value.len);
                }
                const manifest_read_ns = @import("antfly_platform").time.monotonicNs() - manifest_read_start;
                std.debug.print("DOCUMENT_PATCH_READ payload_bytes={d} rounds={d} inline_ns={d} manifest_ns={d}\n", .{ length, read_rounds, inline_read_ns / read_rounds, manifest_read_ns / read_rounds });
                try std.testing.expectError(error.UnsupportedVersion, store.put(schema.schema_key, encoded));
                try store.sync(true);
            }
            if (pass == 1) {
                const started = @import("antfly_platform").time.monotonicNs();
                const recovered = try store.get(a, key);
                defer a.free(recovered);
                const elapsed = @import("antfly_platform").time.monotonicNs() - started;
                try std.testing.expectEqualStrings(second, recovered);
                std.debug.print("DOCUMENT_REOPEN_READ payload_bytes={d} elapsed_ns={d} backend_cache=cold os_cache=uncontrolled\n", .{ length, elapsed });
            }
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualStrings(second, try read.get(key));
            try std.testing.expectEqual(@as(u32, 26), std.mem.readInt(u32, (try read.get(schema.schema_key))[4..8], .little));
            try std.testing.expect((try revision.loadContent(&read, key)).?.manifest);
            // Ordinary export contains complete logical JSON and no private
            // document blobs. A physical checkpoint keeps the underlying keys.
            const exported = try store.scanRange(a, "", "");
            defer DocStore.freeResults(a, exported);
            for (exported) |entry| {
                try std.testing.expect(document_payload.ownership.privatePrefix(entry.key) == null);
                if (std.mem.eql(u8, entry.key, key)) try std.testing.expectEqualStrings(second, entry.value);
            }
        }
    }
}

test "document payload unsupported directories stay inline and shrinking retires only owned values" {
    const a = std.testing.allocator;
    var backend = lsm_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const schema = @import("schema.zig");
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const key = try internal_keys.documentKeyAlloc(a, "wide");
    defer a.free(key);
    var body: std.Io.Writer.Allocating = .init(a);
    defer body.deinit();
    try body.writer.writeByte('{');
    for (0..600) |i| try body.writer.print("\"field{d}\":1,", .{i});
    try body.writer.writeAll("\"large\":\"");
    try body.writer.splatByteAll('x', 128 * 1024);
    try body.writer.writeAll("\"}");
    try store.put(key, body.written());
    try store.put(key, body.written());
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(!document_payload.isManifest(try read.getPhysical(key)));
        try std.testing.expectEqualStrings(body.written(), try read.get(key));
    }
    body.clearRetainingCapacity();
    try body.writer.writeAll("{\"large\":\"");
    try body.writer.splatByteAll('x', 128 * 1024);
    try body.writer.writeAll("\",\"metadata\":1}");
    try store.put(key, body.written());
    try store.put(key, body.written());
    var pinned = try store.beginReadTxn();
    defer pinned.abort();
    try std.testing.expect(document_payload.isManifest(try pinned.getPhysical(key)));
    try store.put(key, "{}");
    var current = try store.beginReadTxn();
    defer current.abort();
    try std.testing.expectEqualStrings("{}", try current.get(key));
    try std.testing.expectEqualStrings(body.written(), try pinned.get(key));
    var cursor = try current.openPhysicalCursorAdapter();
    defer cursor.close();
    const owner_prefix = document_payload.blobKey(key, @splat(0))[0 .. document_payload.prefix.len + 32].*;
    if (try cursor.seekAtOrAfter(&owner_prefix)) |entry| try std.testing.expect(!std.mem.startsWith(u8, entry.key, &owner_prefix));
}

test "document payload logical authorities agree across inline and external layouts" {
    const a = std.testing.allocator;
    const schema = @import("schema.zig");
    const revision = @import("document_mutation_revision.zig");
    const key = try internal_keys.documentKeyAlloc(a, "doc");
    defer a.free(key);
    const payload = try a.alloc(u8, 128 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    const body = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{payload});
    defer a.free(body);
    var authorities: [2][32]u8 = undefined;
    for (0..2) |arm| {
        var backend = lsm_backend.Backend.init(a, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        store.document_payload_prototype = true;
        store.document_payload_inline = arm == 0;
        const encoded = try schema.serializeSchema(a, .{});
        defer a.free(encoded);
        try store.put(schema.schema_key, encoded);
        try store.put(key, body);
        try store.put(key, body);
        var read = try store.beginReadTxn();
        defer read.abort();
        const proof = (try revision.loadContent(&read, key)).?;
        try std.testing.expectEqual(arm == 1, proof.manifest);
        try std.testing.expectEqualStrings(body, try read.get(key));
        authorities[arm] = proof.digest;
        if (arm == 0) {
            try std.testing.expectEqual(@as(u32, 26), std.mem.readInt(u32, (try read.get(schema.schema_key))[4..8], .little));
            var write = try store.beginWriteTxn();
            defer write.abort();
            try std.testing.expectError(error.Unsupported, write.patchMetadata(key, &.{.{ .name = "metadata", .value_json = "2" }}));
        }
    }
    try std.testing.expectEqualSlices(u8, &authorities[0], &authorities[1]);
}

test "document payload authority recovery obeys the pinned schema capability rather than cache presence" {
    const a = std.testing.allocator;
    const revisions = @import("document_mutation_revision.zig");
    const schema = @import("schema.zig");
    var backend = lsm_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const key = try internal_keys.documentKeyAlloc(a, "old");
    defer a.free(key);
    const other = try internal_keys.documentKeyAlloc(a, "new");
    defer a.free(other);
    const payload = try a.alloc(u8, 128 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    const body = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\"}}", .{payload});
    defer a.free(body);
    try store.put(key, body);
    var before = try store.beginReadTxn();
    defer before.abort();
    const legacy = (try revisions.loadContent(&before, key)).?;
    try std.testing.expectEqual(revisions.Algorithm.sha256_json, legacy.algorithm);
    store.document_payload_prototype = true;
    try store.put(other, body);
    // An existing pinned cut retains its original protocol and cache meaning.
    try std.testing.expectEqual(revisions.Algorithm.sha256_json, (try revisions.loadContent(&before, key)).?.algorithm);
    {
        var current = try store.beginReadTxn();
        defer current.abort();
        try std.testing.expect((try revisions.loadContent(&current, key)) == null);
    }
    var page = (try revisions.BackfillPage.prepare(a, &store, true)).?;
    defer page.deinit();
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        _ = try page.stage(&write);
        try write.commit();
    }
    var expected: [32]u8 = undefined;
    {
        var current = try store.beginReadTxn();
        defer current.abort();
        const rebuilt = (try revisions.loadContent(&current, key)).?;
        try std.testing.expectEqual(revisions.Algorithm.logical_json_v1, rebuilt.algorithm);
        try std.testing.expectEqual(legacy.revision, rebuilt.revision);
        try std.testing.expect(!rebuilt.manifest);
        expected = rebuilt.digest;
    }
    try store.delete(&revisions.contentKey(revisions.keyForPhysical(key).?));
    for (0..8) |attempt| {
        var again = (try revisions.BackfillPage.prepare(a, &store, attempt == 0)).?;
        defer again.deinit();
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        _ = try again.stage(&write);
        try write.commit();
        if (again.complete) break;
    }
    var current = try store.beginReadTxn();
    defer current.abort();
    try std.testing.expectEqualSlices(u8, &expected, &(try revisions.loadContent(&current, key)).?.digest);
    try std.testing.expectEqual(revisions.Algorithm.sha256_json, (try revisions.loadContent(&before, key)).?.algorithm);
}

test "document payload retained references survive restart overwrite and bounded reclamation" {
    const a = std.testing.allocator;
    const frame_mod = @import("retained_frame.zig");
    const reference = @import("retained_reference.zig");
    const schema = @import("schema.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const key = try internal_keys.documentKeyAlloc(a, "retained");
    defer a.free(key);
    const body = try a.alloc(u8, 1024 * 1024 + 37);
    defer a.free(body);
    @memset(body, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
    defer a.free(second);
    var blob_key: document_payload.BlobKey = undefined;
    for (0..2) |pass| {
        var backend = try lsm_backend.Backend.open(a, path, .{ .flush_threshold_bytes = 16 * 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        store.document_payload_prototype = true;
        if (pass == 0) {
            const encoded = try schema.serializeSchema(a, .{});
            defer a.free(encoded);
            try store.put(schema.schema_key, encoded);
            try store.put(&internal_keys.identity_namespace_key, &@as(retained_effects.Namespace, @splat(1)));
            try store.put(key, first);
            try store.put(key, second);
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                const manifest = try document_payload.Manifest.decode(a, try read.getPhysical(key));
                defer a.free(manifest.parts);
                const values = try document_payload.immutableValues(a, manifest);
                defer a.free(values);
                try std.testing.expectEqual(@as(usize, 1), values.len);
                blob_key = document_payload.blobKey(key, values[0].digest);
            }
            {
                var write = try store.beginWriteTxn();
                errdefer write.abort();
                _ = try retained_effects.admitWithCapabilities(&write, @splat(1), 1, @splat(3), retained_effects.default_limit, false, true);
                try write.commit();
            }
            for (0..2) |index| {
                var write = try store.beginWriteTxn();
                errdefer write.abort();
                try write.patchMetadata(key, &.{.{ .name = "metadata", .value_json = if (index == 0) "3" else "4" }});
                try write.commit();
            }
            var read = try store.beginReadTxn();
            defer read.abort();
            const state = (try retained_effects.load(&read)).?;
            var stored: usize = 0;
            for (1..3) |seq| {
                const raw = try read.get(&retained_effects.recordKey(seq));
                try std.testing.expect(reference.isRecord(raw));
                try std.testing.expect(raw.len < 2048);
                stored += raw.len;
            }
            try std.testing.expectEqual(@as(u64, body.len + 2 + stored), state.retained_bytes);
            std.debug.print("DOCUMENT_RETAINED payload_bytes={d} frames=2 frame_bytes={d} charged_bytes={d} unique_payloads=1\n", .{ body.len + 2, stored, state.retained_bytes });
        } else {
            // Replacing the current row releases its ownership, but retained
            // frames still own their exact immutable afterimages.
            try store.put(key, "{\"metadata\":5}");
            var historical = try store.beginReadTxn();
            defer historical.abort();
            const scratch = try a.alloc(u8, frame_mod.chunk_bytes);
            defer a.free(scratch);
            var cache: frame_mod.View.ChunkCache = .{ .bytes = scratch };
            const frame = (try retained_effects.readFrame(&historical, @splat(1), 1, @splat(3), 0, &cache)).?;
            const view = frame.chunked;
            const effect = try view.effectAt(0, &cache);
            const reconstructed = try a.alloc(u8, effect.value_len.?);
            defer a.free(reconstructed);
            try std.testing.expectEqual(reconstructed.len, try view.readAt(effect.value_offset, reconstructed, &cache));
            const expected = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":3}}", .{body});
            defer a.free(expected);
            try std.testing.expectEqualStrings(expected, reconstructed);
            // A valid backend record with wrong immutable bytes must not be
            // legitimized by computing fresh transport hashes over corruption.
            const original_blob = try a.dupe(u8, try historical.getPhysical(&blob_key));
            defer a.free(original_blob);
            const corrupt_blob = try a.dupe(u8, original_blob);
            defer a.free(corrupt_blob);
            corrupt_blob[1] = 'z';
            try store.put(&blob_key, corrupt_blob);
            {
                var damaged = try store.beginReadTxn();
                defer damaged.abort();
                cache.invalidate();
                try std.testing.expectError(error.InvalidDocumentManifest, retained_effects.readFrame(&damaged, @splat(1), 1, @splat(3), 0, &cache));
            }
            try store.put(&blob_key, original_blob);
            // GC is atomic and reference-counted, including across reopen.
            {
                var write = try store.beginWriteTxn();
                errdefer write.abort();
                try retained_effects.acknowledge(&write, @splat(1), 1, @splat(3), 0, 2);
                try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaim(&write, @splat(1), 1, 1));
                try write.commit();
            }
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqual(body.len + 2, (try read.getPhysical(&blob_key)).len);
            }
            {
                var write = try store.beginWriteTxn();
                errdefer write.abort();
                try std.testing.expectEqual(@as(usize, 1), try retained_effects.reclaim(&write, @splat(1), 1, 1));
                try write.commit();
            }
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectError(error.NotFound, read.getPhysical(&blob_key));
            }
            // A reader predating GC retains its MVCC payload cut.
            cache.invalidate();
            try std.testing.expectEqual(reconstructed.len, try view.readAt(effect.value_offset, reconstructed, &cache));
            try std.testing.expectEqualStrings(expected, reconstructed);
        }
    }
}

// Advisory eviction is confined to closed, private benchmark files. It does
// not drop a host-wide cache or touch another table's pages. Success means the
// kernel accepted advice, not proof that every storage layer is physically cold.
fn advisePayloadFixtureCold(path: []const u8) !bool {
    if (comptime builtin.os.tag != .linux) return false;
    const io = std.Io.Threaded.global_single_threaded.io();
    const Walk = struct {
        fn apply(dir: std.Io.Dir, runtime_io: std.Io, depth: usize) anyerror!void {
            if (depth > 32) return error.ResourceLimitExceeded;
            var iterator = dir.iterate();
            while (try iterator.next(runtime_io)) |entry| switch (entry.kind) {
                .file => {
                    var file = try dir.openFile(runtime_io, entry.name, .{});
                    defer file.close(runtime_io);
                    try file.sync(runtime_io);
                    if (std.os.linux.errno(std.os.linux.fadvise(file.handle, 0, 0, std.os.linux.POSIX_FADV.DONTNEED)) != .SUCCESS) return error.ColdAdviceFailed;
                },
                .directory => {
                    var child = try dir.openDir(runtime_io, entry.name, .{ .iterate = true });
                    defer child.close(runtime_io);
                    try apply(child, runtime_io, depth + 1);
                },
                else => {},
            };
        }
    };
    var directory = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer directory.close(io);
    try Walk.apply(directory, io, 0);
    return true;
}

test "document payload mixed workload qualification compares inline and adaptive layouts" {
    if (!@import("builtin").link_libc or std.c.getenv("ANTFLY_BENCH_DOCUMENT_MIXED") == null) return error.SkipZigTest;
    const allocation_mode: []const u8 = if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_ALLOCATOR")) |raw| std.mem.span(raw) else "production";
    if (!std.mem.eql(u8, allocation_mode, "production") and !std.mem.eql(u8, allocation_mode, "checked")) return error.InvalidArgument;
    const a = if (std.mem.eql(u8, allocation_mode, "checked")) std.testing.allocator else @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator);
    const schema = @import("schema.zig");
    const row_count: usize = if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_ROWS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 1;
    if (row_count == 0 or row_count > 256) return error.InvalidArgument;
    const row_keys = try a.alloc([]u8, row_count);
    defer a.free(row_keys);
    var initialized: usize = 0;
    defer for (row_keys[0..initialized]) |key| a.free(key);
    for (row_keys) |*key| {
        const id = try std.fmt.allocPrint(a, "mixed-{d}", .{initialized});
        defer a.free(id);
        key.* = try internal_keys.documentKeyAlloc(a, if (row_count == 1) "mixed" else id);
        initialized += 1;
    }
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    const rounds: usize = if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_ROUNDS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 6;
    if (rounds < 2 or rounds > 1024) return error.InvalidArgument;
    const selected_bytes: ?usize = if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_BYTES")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else null;
    const selected_reads: ?usize = if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_READS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else null;
    const reverse = std.c.getenv("ANTFLY_BENCH_DOCUMENT_REVERSE") != null;
    for ([_]usize{ 4 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024 }) |length| {
        if (selected_bytes != null and selected_bytes.? != length) continue;
        const body = try a.alloc(u8, length);
        defer a.free(body);
        if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_ENTROPY") != null) {
            var random = std.Random.DefaultPrng.init(0x41464c59);
            random.random().bytes(body);
            for (body) |*byte| byte.* = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[byte.* & 63];
        } else @memset(body, 'x');
        const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
        defer a.free(first);
        const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
        defer a.free(second);
        const changed = try a.dupe(u8, first);
        defer a.free(changed);
        changed[10] = if (changed[10] == 'y') 'z' else 'y';
        const changed_second = try a.dupe(u8, second);
        defer a.free(changed_second);
        changed_second[10] = if (changed_second[10] == 'y') 'z' else 'y';
        for ([_]usize{ 0, 8, 64 }) |reads_per_write| for ([_]usize{ 0, 1, 2 }) |change_mode| for (0..3) |slot| {
            if (selected_reads != null and selected_reads.? != reads_per_write) continue;
            const changing = change_mode != 0;
            const arm = if (reverse) 2 - slot else slot;
            const layout = switch (arm) {
                0 => "production",
                1 => "inline",
                else => "adaptive",
            };
            if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_LAYOUT")) |selected| if (!std.mem.eql(u8, std.mem.span(selected), layout)) continue;
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path});
            {
                var backend = try lsm_backend.Backend.open(a, path, .{ .backend = .{ .durability = .full }, .flush_threshold_bytes = 16 * 1024 * 1024 });
                defer backend.close();
                var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
                defer store.close();
                store.document_payload_prototype = arm != 0;
                store.document_payload_inline = arm == 1;
                try store.put(schema.schema_key, encoded);
                for (row_keys) |key| {
                    try store.put(key, first);
                    try store.put(key, second);
                }
                // The fixture has no background executor. Drive the ordinary
                // admitted maintenance path explicitly for multi-row workloads.
                if (row_count > 1) {
                    var steps: usize = 0;
                    while (try backend.runMaintenanceStep()) {
                        steps += 1;
                        if (steps > 1024) return error.ResourceLimitExceeded;
                    }
                }
                const before = backend.snapshotWriteStats();
                const begin = @import("antfly_platform").time.monotonicNs();
                var patched: usize = 0;
                var commit_syncs: usize = 0;
                for (0..rounds) |operation| {
                    const key = row_keys[operation % row_count];
                    const round = operation / row_count;
                    const sync_before = backend.snapshotWriteStats().wal_sync_records;
                    if (arm == 2 and !changing and length >= document_payload.minimum_value_bytes) {
                        var write = try store.beginWriteTxn();
                        var finished = false;
                        defer if (!finished) write.abort();
                        if (write.patchMetadata(key, &.{.{ .name = "metadata", .value_json = if (round % 2 == 0) "1" else "2" }})) |_| {
                            try write.commit();
                            finished = true;
                            patched += 1;
                        } else |err| {
                            write.abort();
                            finished = true;
                            if (err != error.Unsupported) return err;
                            try store.put(key, if (round % 2 == 0) first else second);
                        }
                    } else try store.put(key, if (change_mode == 2)
                        (if ((round / 3) % 2 == 0) (if (round % 2 == 0) changed else changed_second) else if (round % 2 == 0) first else second)
                    else if (changing) (if (round % 2 == 0) changed else first) else if (round % 2 == 0) first else second);
                    try std.testing.expect(backend.snapshotWriteStats().wal_sync_records > sync_before);
                    commit_syncs += 1;
                    if (row_count > 1) _ = try backend.runMaintenanceStep();
                    for (0..reads_per_write) |_| {
                        const result = try store.get(a, key);
                        defer a.free(result);
                        try std.testing.expectEqual(first.len, result.len);
                    }
                }
                const elapsed = @import("antfly_platform").time.monotonicNs() - begin;
                const after = backend.snapshotWriteStats();
                try std.testing.expectEqual(rounds, commit_syncs);
                try std.testing.expect(after.wal_sync_records - before.wal_sync_records >= rounds);
                const sync_begin = @import("antfly_platform").time.monotonicNs();
                try store.sync(true);
                if (row_count > 1) {
                    var steps: usize = 0;
                    while (try backend.runMaintenanceStep()) {
                        steps += 1;
                        if (steps > 1024) return error.ResourceLimitExceeded;
                    }
                }
                const sync_ns = @import("antfly_platform").time.monotonicNs() - sync_begin;
                const durable = backend.snapshotWriteStats();
                std.debug.print("DOCUMENT_MIXED layout={s} allocator={s} payload={d} changing={d} reads_per_write={d} rounds={d} row_count={d} commit_syncs={d} elapsed_ns={d} wal_bytes={d} table_bytes={d} sync_ns={d} patches={d} wal_syncs={d} compactions={d} compaction_bytes={d}\n", .{
                    layout,                                           allocation_mode,                                    length,  change_mode, reads_per_write,                                  rounds,                                   row_count,                                                        commit_syncs, elapsed,
                    after.wal_append_bytes - before.wal_append_bytes, durable.table_file_bytes - before.table_file_bytes, sync_ns, patched,     after.wal_sync_records - before.wal_sync_records, durable.compactions - before.compactions, durable.compaction_output_bytes - before.compaction_output_bytes,
                });
            }
            // Separate engine lifetime: no block cache or decoded manifest
            // survives. OS cache state is reported explicitly, not called cold.
            const advised_cold = if (std.c.getenv("ANTFLY_BENCH_DOCUMENT_COLD") != null) try advisePayloadFixtureCold(path) else false;
            const cold_begin = @import("antfly_platform").time.monotonicNs();
            var recovered_backend = try lsm_backend.Backend.open(a, path, .{ .backend = .{ .durability = .full }, .flush_threshold_bytes = 16 * 1024 * 1024 });
            defer recovered_backend.close();
            var recovered_store = try DocStore.openRuntime(a, try recovered_backend.runtimeStore(a, .{}));
            defer recovered_store.close();
            recovered_store.document_payload_prototype = arm != 0;
            const recovered_value = try recovered_store.get(a, row_keys[(rounds - 1) % row_count]);
            defer a.free(recovered_value);
            const cold_elapsed = @import("antfly_platform").time.monotonicNs() - cold_begin;
            try std.testing.expectEqual(first.len, recovered_value.len);
            std.debug.print("DOCUMENT_COLD layout={s} allocator={s} payload={d} changing={d} reads_per_write={d} row_count={d} elapsed_ns={d} engine_cache=cold os_cache={s} operation=reopen_and_read\n", .{ layout, allocation_mode, length, change_mode, reads_per_write, row_count, cold_elapsed, if (advised_cold) "advised_cold" else "uncontrolled" });
        };
    }
}

test "document payload provider observation preserves pinned source revisions" {
    const input = @import("document_input.zig");
    const schema = @import("schema.zig");
    const benchmark = builtin.link_libc and std.c.getenv("ANTFLY_BENCH_DOCUMENT_INPUT") != null;
    const a = if (benchmark) platform.allocator.processAllocator(std.heap.page_allocator) else std.testing.allocator;
    const length: usize = if (benchmark) 1024 * 1024 else 128 * 1024;
    for ([_]bool{ false, true }) |entropy| {
        var backend = lsm_backend.Backend.init(a, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        store.document_payload_prototype = true;
        const encoded = try schema.serializeSchema(a, .{});
        defer a.free(encoded);
        try store.put(schema.schema_key, encoded);
        const key = try internal_keys.documentKeyAlloc(a, "provider\x00row");
        defer a.free(key);
        const body = try a.alloc(u8, length);
        defer a.free(body);
        if (entropy) {
            var random = std.Random.DefaultPrng.init(0x41464c59);
            random.random().bytes(body);
            for (body) |*byte| byte.* = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[byte.* & 63];
        } else @memset(body, 'x');
        const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
        defer a.free(first);
        const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
        defer a.free(second);
        try store.put(key, first);
        try store.put(key, second);
        {
            var verify = try store.beginReadTxn();
            defer verify.abort();
            try std.testing.expect(document_payload.isManifest(try verify.getPhysical(key)));
        }
        const Materialize = struct {
            fn copy(_: *anyopaque, alloc: Allocator, _: []const u8, bytes: []const u8, observed: []const u8) !void {
                const logical = try alloc.dupe(u8, bytes);
                defer alloc.free(logical);
                if (!std.mem.eql(u8, logical, observed)) return error.EnrichmentSourceChanged;
            }
            fn borrow(_: *anyopaque, _: Allocator, _: []const u8, bytes: []const u8, observed: []const u8) !void {
                if (!std.mem.eql(u8, bytes, observed)) return error.EnrichmentSourceChanged;
            }
        };
        const port: input.Verifier = .{ .ptr = &store, .verify = Materialize.copy };
        var expected: input.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(second, &expected, .{});
        var pinned = try store.beginReadTxn();
        defer pinned.abort();
        try std.testing.expectEqualDeep(expected, try input.Qualification.nativeDigest(a, &pinned, key, second, port));
        // Neither a new commit nor a demotion/replacement of the current row
        // changes the source bound to an already captured immutable view.
        try store.put(key, first);
        try std.testing.expectEqualDeep(expected, try input.Qualification.nativeDigest(a, &pinned, key, second, port));
        try std.testing.expectError(error.EnrichmentSourceChanged, input.Qualification.nativeDigest(a, &pinned, key, first, port));
        if (benchmark) {
            const LogicalOnly = struct {
                reader: *DocStore.Txn,
                pub fn get(self: *@This(), candidate: []const u8) ![]const u8 {
                    return self.reader.get(candidate);
                }
            };
            // Alternate order to expose warm-cache/order effects. This is a
            // focused input-check benchmark, not a whole provider workload.
            for (0..7) |trial| {
                var timings: [3]u64 = undefined;
                for (0..3) |pass| {
                    const mode = (trial + pass) % 3;
                    const started = platform_time.monotonicNs();
                    for (0..32) |_| {
                        var read = try store.beginReadTxn();
                        defer read.abort();
                        const actual = if (mode == 0) blk: {
                            var logical = LogicalOnly{ .reader = &read };
                            break :blk try input.captureDigest(a, &logical, key, first, port);
                        } else if (mode == 1) try input.Qualification.nativeDigest(a, &read, key, first, port) else try input.captureDigest(a, &read, key, first, .{ .ptr = &store, .verify = Materialize.borrow });
                        std.mem.doNotOptimizeAway(actual);
                    }
                    timings[mode] = platform_time.monotonicNs() - started;
                }
                std.debug.print("DOCUMENT_INPUT allocator=production dataset={s} trial={d} bytes={d} rounds=32 logical_ns={d} manifest_ns={d} borrow_ns={d}\n", .{ if (entropy) "entropy" else "repeated", trial, length, timings[0], timings[1], timings[2] });
            }
        }
    }
}

test "document payload admission hysteresis persists through changing and reused values" {
    const a = std.testing.allocator;
    const schema = @import("schema.zig");
    var backend = lsm_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const key = try internal_keys.documentKeyAlloc(a, "cooldown");
    defer a.free(key);
    const small = try internal_keys.documentKeyAlloc(a, "small");
    defer a.free(small);
    try store.put(small, "{\"a\":1}");
    try std.testing.expectEqual(@as(u8, 1), store.document_values_possible.load(.acquire));
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    for (0..7) |step| {
        if (step >= 2) body[0] = 'y';
        const source = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":{d}}}", .{ body, step });
        defer a.free(source);
        try store.put(key, source);
        var read = try store.beginReadTxn();
        defer read.abort();
        const proof = (try revisionForPayloadTest(&read, key)).?;
        try std.testing.expectEqual(step == 1 or step == 6, proof.manifest);
        try std.testing.expectEqual(@as(u2, if (step >= 2 and step <= 4) @intCast(5 - step) else 0), proof.layout_cooldown);
        try std.testing.expectEqualStrings(source, try read.get(key));
        if (step >= 1) try std.testing.expectEqual(@as(u8, 2), store.document_values_possible.load(.acquire));
    }
    // Dropping a positive hint after a generation replacement must reload the
    // upgraded format, rather than infer that this particular small row is safe.
    store.document_values_possible.store(0, .release);
    try store.put(small, "{\"a\":2}");
    try std.testing.expectEqual(@as(u8, 2), store.document_values_possible.load(.acquire));
}

test "document payload prepared logical writes preserve observers and fallback after input changes" {
    const a = std.testing.allocator;
    const schema = @import("schema.zig");
    var backend = lsm_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try store.put(schema.schema_key, encoded);
    const key = try internal_keys.documentKeyAlloc(a, "prepared");
    defer a.free(key);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
    defer a.free(second);
    const next = try std.fmt.allocPrint(a, "{{\"metadata\":3,\"body\":\"{s}\"}}", .{body});
    defer a.free(next);
    try store.put(key, first);
    try store.put(key, second);
    const revisions = @import("document_mutation_revision.zig");
    const cache_key = revisions.contentKey(revisions.keyForPhysical(key).?);
    const stale = try store.get(a, &cache_key);
    defer a.free(stale);
    stale[16] &= ~@as(u8, 0x80);
    try store.put(&cache_key, stale);
    var read = try store.beginReadTxn();
    defer read.abort();
    const source = (try document_payload.SourceMetadata.capture(a, &read, key)).?;
    defer source.deinit(a);
    const prepared = (try source.prepare(a, second, next, &.{.{ .name = "metadata", .value_json = "3" }}, &.{ "metadata", "body" })).?;
    defer prepared.deinit(a);
    // The logical root is unchanged by framing, but the old raw references
    // belong to one exact manifest. Such a rewrite must decline the receipt.
    const reframed = try std.fmt.allocPrint(a, "{{ \"body\":\"{s}\", \"metadata\":2 }}", .{body});
    defer a.free(reframed);
    try store.put(key, reframed);
    {
        var changed_layout = try store.beginReadTxn();
        defer changed_layout.abort();
        const proof = (try revisions.loadContent(&changed_layout, key)).?;
        try std.testing.expectEqualSlices(u8, &source.proof.digest, &proof.digest);
        try std.testing.expect(!try prepared.qualifies(&changed_layout, key, next));
    }
    try store.put(key, second);
    const Observer = struct {
        key: []const u8,
        expected: []const u8,
        saw_source: bool = false,
        prepared: *const document_payload.PreparedWrite,
        pub fn find(ptr: *const anyopaque, name: []const u8) ?*const document_payload.PreparedWrite {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            return if (std.mem.eql(u8, name, self.key)) self.prepared else null;
        }
        pub fn reset(_: *anyopaque) void {}
        pub fn observe(ptr: *anyopaque, name: []const u8, value: ?[]const u8) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, name, self.key)) self.saw_source = value != null and std.mem.eql(u8, value.?, self.expected);
        }
        pub fn stage(_: *anyopaque, _: @import("commit_participant.zig").View, _: []const u8) !void {}
    };
    var observer: Observer = .{ .key = key, .expected = next, .prepared = &prepared };
    const participant = @import("commit_participant.zig").Participant.withDocumentPreparations(
        .{ .ptr = &observer, .reset = Observer.reset, .observe = Observer.observe, .stage = Observer.stage },
        .{ .ptr = &observer, .find = Observer.find },
    );
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try write.artifact_inputs.attach(participant);
        try write.put(key, next);
        try std.testing.expect(observer.saw_source);
        try std.testing.expectEqual(@as(usize, 0), write.document_revisions.identity_reuse.canonical_bytes);
        try std.testing.expectEqual(@as(usize, 0), write.document_revisions.document_payload_stats.blob_bytes_written);
        try write.commit();
    }
    const actual = try store.get(a, key);
    defer a.free(actual);
    try std.testing.expectEqualStrings(next, actual);
    const wire = try std.json.Stringify.valueAlloc(a, KVPair{ .key = key, .value = next }, .{});
    defer a.free(wire);
    try std.testing.expect(std.mem.indexOf(u8, wire, "prepared_document") == null);
    const changed = try std.fmt.allocPrint(a, "{{\"metadata\":4,\"body\":\"{s}\"}}", .{body});
    defer a.free(changed);
    observer.expected = changed;
    observer.saw_source = false;
    try store.putBatchWithReplayAndParticipant(null, &.{.{ .key = key, .value = changed }}, &.{}, null, .{}, participant);
    try std.testing.expect(observer.saw_source);
    const fallback = try store.get(a, key);
    defer a.free(fallback);
    try std.testing.expectEqualStrings(changed, fallback);
    const final_cache = try store.get(a, &cache_key);
    defer a.free(final_cache);
    final_cache[16] &= ~@as(u8, 0x80);
    try store.put(&cache_key, final_cache);
    const prior_manifest = try document_payload.Manifest.decode(a, source.manifest);
    defer a.free(prior_manifest.parts);
    const blob = for (prior_manifest.parts) |part| {
        if (part == .blob) break document_payload.blobKey(key, part.blob.digest);
    } else return error.TestUnexpectedResult;
    try store.put(key, "{\"metadata\":0}");
    var retired = try store.beginReadTxn();
    defer retired.abort();
    try std.testing.expectError(error.NotFound, retired.getPhysical(&blob));
}

test "document payload keys-only logical cursors never resolve values from fallback adapters" {
    const a = std.testing.allocator;
    const physical_key = try internal_keys.documentKeyAlloc(a, "fallback");
    defer a.free(physical_key);
    const Fallback = struct {
        key: []const u8,
        value: []const u8,
        closed: *usize,
        pub fn close(self: *@This()) void {
            self.closed.* += 1;
        }
        pub fn first(self: *@This()) !?backend_erased.Entry {
            return .{ .key = self.key, .value = self.value };
        }
        pub const last = first;
        pub fn next(_: *@This()) !?backend_erased.Entry {
            return null;
        }
        pub const prev = next;
        pub fn seekAtOrAfter(self: *@This(), _: []const u8) !?backend_erased.Entry {
            return self.first();
        }
        pub const seekAtOrBefore = seekAtOrAfter;
        // Intentionally has no setKeysOnly: the logical adapter must still
        // return empty values without requiring a native source reader.
    };
    for ([_][]const u8{ document_payload.magic, "ordinary inline value" }, 0..) |raw, index| {
        var closed: usize = 0;
        {
            var physical = try backend_erased.cursorFrom(a, Fallback{ .key = physical_key, .value = raw, .closed = &closed });
            var cursor = DocStore.wrapPayloadCursor(a, physical, null, false, null, null, null, false) catch |err| {
                physical.close();
                return err;
            };
            defer cursor.close();
            if (index == 0) try std.testing.expectError(error.Unsupported, cursor.first());
            cursor.setKeysOnly();
            for ([_]?backend_erased.Entry{ try cursor.first(), try cursor.last(), try cursor.seekAtOrAfter(physical_key), try cursor.seekAtOrBefore(physical_key) }) |entry| {
                try std.testing.expectEqualStrings(physical_key, entry.?.key);
                try std.testing.expectEqual(@as(usize, 0), entry.?.value.len);
            }
            try std.testing.expect(try cursor.next() == null);
            try std.testing.expect(try cursor.prev() == null);
        }
        try std.testing.expectEqual(@as(usize, 1), closed);
    }
}

test "document payload native receipts cannot be constructed through JSON codecs" {
    const a = std.testing.allocator;
    const wire = "{\"key\":\"doc\",\"value\":\"{}\"}";
    var decoded = try std.json.parseFromSlice(KVPair, a, wire, .{});
    defer decoded.deinit();
    try std.testing.expect(!@hasField(KVPair, "prepared_document"));
    try std.testing.expectEqual(2 * @sizeOf([]const u8), @sizeOf(KVPair));
    try std.testing.expectEqualStrings("doc", decoded.value.key);
    const injected = "{\"key\":\"doc\",\"value\":\"{}\",\"prepared_document\":{\"expected_json\":\"forged\"}}";
    try std.testing.expectError(error.UnknownField, std.json.parseFromSlice(KVPair, a, injected, .{}));
    var ignored = try @import("antfly-json").parseFromSlice(KVPair, a, injected, .{ .ignore_unknown_fields = true });
    defer ignored.deinit();
    try std.testing.expectEqualStrings("{}", ignored.value.value);
    var tree = try std.json.parseFromSlice(std.json.Value, a, injected, .{});
    defer tree.deinit();
    try std.testing.expectError(error.UnknownField, std.json.parseFromValue(KVPair, a, tree.value, .{}));
    var ignored_tree = try std.json.parseFromValue(KVPair, a, tree.value, .{ .ignore_unknown_fields = true });
    defer ignored_tree.deinit();
    try std.testing.expectEqualStrings("doc", ignored_tree.value.key);
    try std.testing.expectEqualStrings("{}", ignored_tree.value.value);
}
