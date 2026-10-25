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

//! Split operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDb = db_zig.TestDb;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const db_config = @import("config.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const docstore_mod = @import("../docstore.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const graph_mod = @import("../../graph/graph.zig");
const index_generation_manifest = @import("derived/index_generation_manifest.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const public_table_schema = @import("../../schema/mod.zig");
const range_cardinality = @import("range_cardinality.zig");
const range_state_mod = @import("range_state.zig");
const shard_mod = @import("../shard.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const types = @import("types.zig");
const Allocator = db_zig.Allocator;
const DbSplitSimRuntime = db_zig.DbSplitSimRuntime;
const PrimaryBackend = db_zig.PrimaryBackend;
const ShadowState = db_zig.ShadowState;
const byteRangesEqual = db_zig.byteRangesEqual;
const cleanupTempDir = db_zig.cleanupTempDir;
const db_split_sim_index_name = db_zig.db_split_sim_index_name;
const db_split_sim_split_key = db_zig.db_split_sim_split_key;
const ensureDirPath = db_zig.ensureDirPath;
const finalizeSplitLocked = db_zig.finalizeSplitLocked;
const graph_split_retire_cursor_key = db_zig.graph_split_retire_cursor_key;
const lockApplyShared = db_zig.lockApplyShared;
const lockAtomicWithBackoff = db_zig.lockAtomicWithBackoff;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const no_background_test_open = db_zig.no_background_test_open;
const prepareSplitDestination = db_zig.prepareSplitDestination;
const recoverInterruptedGraphSplitRetirement = db_zig.recoverInterruptedGraphSplitRetirement;
const runDbSplitReplayCase = db_zig.runDbSplitReplayCase;
const runDbSplitReplayFixtures = db_zig.runDbSplitReplayFixtures;
const runModeledDbSplitReplayCase = db_zig.runModeledDbSplitReplayCase;
const runModeledDbSplitReplayFixtures = db_zig.runModeledDbSplitReplayFixtures;
const splitBootstrapMarkersEqual = db_zig.splitBootstrapMarkersEqual;
const waitForDerivedReplayTarget = db_zig.waitForDerivedReplayTarget;
const waitForSearchResult = db_zig.waitForSearchResult;
const waitForSplitShadowDrainLocked = db_zig.waitForSplitShadowDrainLocked;

pub fn shadowIndexPhysicalFormat(
    shadow_manager: *index_manager_mod.IndexManager,
    index_ref: index_manager_mod.ManagedIndexRef,
) !index_generation_manifest.PhysicalFormat {
    if (index_ref.kind != .dense_vector) return .legacy_lsm;
    const entry = shadow_manager.denseIndex(index_ref.name) orelse return error.IndexNotFound;
    return if (entry.index.experimentalPostingWalAuthoritative())
        .dense_native_v2
    else
        .legacy_lsm;
}

pub fn getSplitState(self: *DB, alloc: Allocator) !?types.SplitState {
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    const state = self.core.splitState() orelse return null;
    return .{
        .phase = state.phase,
        .split_key = try alloc.dupe(u8, state.split_key),
        .new_shard_id = state.new_shard_id,
        .started_at = state.started_at,
        .original_range_end = try alloc.dupe(u8, state.original_range_end),
    };
}

pub fn setSplitState(self: *DB, state: ?types.SplitState) !void {
    if (self.local_execution.source_vectors.load(.acquire) != null) return error.VectorStoreLifecycleUnsupported;
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    if (state == null) {
        try recoverInterruptedGraphSplitRetirement(self);
        try self.core.setSplitState(null);
        return;
    }
    try recoverInterruptedGraphSplitRetirement(self);
    try self.requireSupportedRelationalTopology();
    try self.core.setSplitState(.{
        .phase = state.?.phase,
        .split_key = state.?.split_key,
        .new_shard_id = state.?.new_shard_id,
        .started_at = state.?.started_at,
        .original_range_end = state.?.original_range_end,
    });
}

pub fn clearSplitState(self: *DB) !void {
    try self.setSplitState(null);
}

pub fn getSplitDeltaSeq(self: *DB) u64 {
    return self.core.splitDeltaSequence();
}

pub fn getSplitDeltaFinalSeq(self: *DB, alloc: Allocator) !u64 {
    return try self.core.loadSplitDeltaFinalSeq(alloc);
}

pub fn setSplitDeltaFinalSeq(self: *DB, seq: u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, seq });
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    try self.core.saveSplitDeltaFinalSeq(seq);
}

pub fn clearSplitDeltaFinalSeq(self: *DB) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    try self.core.clearSplitDeltaFinalSeq();
}

pub fn getSplitBootstrapMarker(self: *DB, alloc: Allocator) !?range_state_mod.SplitBootstrapMarker {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return try self.core.loadSplitBootstrapMarker(alloc);
}

pub fn setSplitBootstrapMarker(self: *DB, marker: range_state_mod.SplitBootstrapMarker) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, marker });
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    try self.core.saveSplitBootstrapMarker(marker);
}

/// Atomically starts or replaces a split destination bootstrap. An
/// incomplete marker fences ordinary replication while bounded bootstrap
/// chunks are streamed. Callers must hold the destination generation's
/// exclusive lifecycle lease.
pub fn replaceSplitBootstrap(
    self: *DB,
    alloc: Allocator,
    byte_range: types.ByteRange,
    writes: []const types.BatchWrite,
    base_delta_sequence: u64,
    marker: range_state_mod.SplitBootstrapMarker,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, byte_range, writes, base_delta_sequence, marker });
    lockAtomicWithBackoff(&self.generation_replace_mutex);
    defer self.generation_replace_mutex.unlock();
    try self.requireSupportedRelationalTopology();

    if (marker.transition_id == 0 or marker.attempt_epoch == 0 or
        marker.source_group_id == 0 or marker.destination_group_id == 0 or
        marker.bootstrap_complete)
    {
        return error.InvalidSplitBootstrapMarker;
    }
    if (byte_range.start.len > 0 and byte_range.end.len > 0 and
        std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
    {
        return error.InvalidAppliedDataRange;
    }
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.ensureTotalCapacity(alloc, @intCast(writes.len));
    for (writes) |write| {
        if (!byte_range.contains(write.key)) return error.KeyOutOfRange;
        const result = try seen.getOrPut(alloc, write.key);
        if (result.found_existing) return error.DuplicateDocumentKey;
    }

    if (try self.getSplitBootstrapMarker(alloc)) |existing| {
        const same_attempt = existing.transition_id == marker.transition_id and
            existing.attempt_epoch == marker.attempt_epoch and
            existing.source_group_id == marker.source_group_id and
            existing.destination_group_id == marker.destination_group_id;
        if (same_attempt) {
            const reserved_sequence = try self.getSplitDeltaFinalSeq(alloc);
            if (base_delta_sequence < reserved_sequence) return error.StaleSplitBootstrap;
            if (!byteRangesEqual(self.core.byteRange(), byte_range)) return error.ConflictingSplitTransition;
            if (existing.bootstrap_complete) {
                if (base_delta_sequence > reserved_sequence) return error.SplitBootstrapComplete;
                return false;
            }
            // Source leadership can change after an earlier worker has
            // reserved and populated this exact transfer. Replaying begin
            // must not erase those chunks beneath its pending completion.
            if (base_delta_sequence == reserved_sequence) return false;
        }
        if (!same_attempt and
            (existing.source_group_id != marker.source_group_id or
                existing.destination_group_id != marker.destination_group_id or
                existing.attempt_epoch >= marker.attempt_epoch))
        {
            return error.ConflictingSplitTransition;
        }
    }

    const lower = try internal_keys.documentRangeLowerAlloc(alloc, "");
    defer alloc.free(lower);
    const upper = (try internal_keys.documentRangeUpperAlloc(alloc, "")) orelse return error.InvalidAppliedDataRange;
    defer alloc.free(upper);
    lockApplyShared(self);
    const existing_documents = self.core.scanStoreRange(alloc, lower, upper) catch |err| {
        self.core.unlockApplyShared();
        return err;
    };
    self.core.unlockApplyShared();
    defer docstore_mod.DocStore.freeResults(alloc, existing_documents);

    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| alloc.free(key);
        deletes.deinit(alloc);
    }
    for (existing_documents) |entry| {
        const logical_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, entry.key)) orelse continue;
        if (seen.contains(logical_key)) {
            alloc.free(logical_key);
            continue;
        }
        errdefer alloc.free(logical_key);
        try deletes.append(alloc, logical_key);
    }

    const range_value = try range_state_mod.encodeRangeAlloc(alloc, byte_range);
    defer alloc.free(range_value);
    var sequence_buf: [8]u8 = undefined;
    var marker_buf: [4 * @sizeOf(u64) + 1]u8 = undefined;
    const metadata_writes = try range_state_mod.splitBootstrapMetadataWrites(
        range_value,
        base_delta_sequence,
        marker,
        &sequence_buf,
        &marker_buf,
    );
    self.batchInternal(.{
        .writes = writes,
        .deletes = deletes.items,
        .sync_level = .write,
    }, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
        .extra_store_writes = &metadata_writes,
    }) catch |err| {
        if (try self.getSplitBootstrapMarker(alloc)) |committed| {
            if (splitBootstrapMarkersEqual(committed, marker)) {
                try self.refreshSplitBootstrapRangeInMemory(byte_range);
                return true;
            }
        }
        return err;
    };
    try self.refreshSplitBootstrapRangeInMemory(byte_range);
    return true;
}

/// Atomically publishes a fully streamed split bootstrap. Completion is
/// valid only for the exact reservation installed by replaceSplitBootstrap.
pub fn completeSplitBootstrap(
    self: *DB,
    alloc: Allocator,
    byte_range: types.ByteRange,
    base_delta_sequence: u64,
    marker: range_state_mod.SplitBootstrapMarker,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, byte_range, base_delta_sequence, marker });
    lockAtomicWithBackoff(&self.generation_replace_mutex);
    defer self.generation_replace_mutex.unlock();

    if (marker.transition_id == 0 or marker.attempt_epoch == 0 or
        marker.source_group_id == 0 or marker.destination_group_id == 0 or
        !marker.bootstrap_complete)
    {
        return error.InvalidSplitBootstrapMarker;
    }
    if (byte_range.start.len > 0 and byte_range.end.len > 0 and
        std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
    {
        return error.InvalidAppliedDataRange;
    }

    const existing = (try self.getSplitBootstrapMarker(alloc)) orelse
        return error.SplitBootstrapRequired;
    if (existing.transition_id != marker.transition_id or
        existing.attempt_epoch != marker.attempt_epoch or
        existing.source_group_id != marker.source_group_id or
        existing.destination_group_id != marker.destination_group_id)
    {
        return error.ConflictingSplitTransition;
    }
    const reserved_sequence = try self.getSplitDeltaFinalSeq(alloc);
    if (base_delta_sequence != reserved_sequence) return error.StaleSplitBootstrap;
    if (!byteRangesEqual(self.core.byteRange(), byte_range)) return error.ConflictingSplitTransition;
    if (existing.bootstrap_complete) {
        try self.refreshSplitBootstrapRangeInMemory(byte_range);
        return false;
    }

    const range_value = try range_state_mod.encodeRangeAlloc(alloc, byte_range);
    defer alloc.free(range_value);
    var sequence_buf: [8]u8 = undefined;
    var marker_buf: [4 * @sizeOf(u64) + 1]u8 = undefined;
    const metadata_writes = try range_state_mod.splitBootstrapMetadataWrites(
        range_value,
        base_delta_sequence,
        marker,
        &sequence_buf,
        &marker_buf,
    );
    try self.batchInternal(.{ .sync_level = .write }, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
        .extra_store_writes = &metadata_writes,
    });
    try self.refreshSplitBootstrapRangeInMemory(byte_range);
    return true;
}

pub fn refreshSplitBootstrapRangeInMemory(self: *DB, byte_range: types.ByteRange) !void {
    const start = try self.alloc.dupe(u8, byte_range.start);
    errdefer self.alloc.free(start);
    const end = try self.alloc.dupe(u8, byte_range.end);
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    self.core.replaceRangeInMemoryOwned(start, end);
}

pub fn clearSplitBootstrapMarker(self: *DB) !void {
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    try self.core.clearSplitBootstrapMarker();
}

pub fn listSplitDeltaEntriesAfter(self: *DB, alloc: Allocator, after_seq: u64) ![]types.SplitDeltaEntry {
    lockApplyShared(self);
    defer self.core.unlockApplyShared();

    const deltas = try self.core.listSplitDeltasAfter(alloc, after_seq);
    defer shard_mod.freeDeltas(alloc, deltas);

    var entries = std.ArrayListUnmanaged(types.SplitDeltaEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(alloc);
        entries.deinit(alloc);
    }

    for (deltas) |delta| {
        var writes = try alloc.alloc(types.BatchWrite, delta.writes.len);
        errdefer alloc.free(writes);
        for (delta.writes, 0..) |write, i| {
            writes[i] = .{
                .key = try alloc.dupe(u8, write.key),
                .value = try alloc.dupe(u8, write.value),
            };
        }

        var deletes = try alloc.alloc([]u8, delta.deletes.len);
        errdefer {
            for (deletes) |key| alloc.free(key);
            alloc.free(deletes);
        }
        for (delta.deletes, 0..) |key, i| {
            deletes[i] = try alloc.dupe(u8, key);
        }

        try entries.append(alloc, .{
            .sequence = delta.sequence,
            .timestamp = delta.timestamp,
            .writes = writes,
            .deletes = deletes,
        });
    }

    return try entries.toOwnedSlice(alloc);
}

pub fn clearSplitDeltaEntries(self: *DB) !void {
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    try self.core.clearSplitDeltas();
}

pub fn createShadowIndexManager(self: *DB, split_key: []const u8, original_range_end: []const u8) !void {
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    if (self.shadow != null) return error.ShadowIndexManagerExists;

    const base_path = try std.fmt.allocPrint(self.alloc, "{s}/.shadow-{d}", .{ self.core.path, monotonicTimeNs() });
    errdefer self.alloc.free(base_path);
    const indexes_path = try std.fmt.allocPrint(self.alloc, "{s}/indexes", .{base_path});
    errdefer self.alloc.free(indexes_path);

    try ensureDirPath(indexes_path);

    const shadow_manager = try self.alloc.create(index_manager_mod.IndexManager);
    errdefer self.alloc.destroy(shadow_manager);
    shadow_manager.* = try index_manager_mod.IndexManager.initWithOptions(
        self.alloc,
        base_path,
        self.index_backends,
    );
    errdefer shadow_manager.deinit();

    const shadow_start = try self.alloc.dupe(u8, split_key);
    errdefer self.alloc.free(shadow_start);
    const shadow_end = try self.alloc.dupe(u8, original_range_end);
    errdefer self.alloc.free(shadow_end);
    shadow_manager.updateRange(.{ .start = shadow_start, .end = shadow_end });

    try self.core.registerShadowIndexes(self.alloc, shadow_manager);

    const shadow = try self.alloc.create(ShadowState);
    shadow.* = .{
        .manager = shadow_manager,
        .base_path = base_path,
        .indexes_path = indexes_path,
        .range_start = shadow_start,
        .range_end = shadow_end,
    };
    self.shadow = shadow;
    if (self.transaction_recovery_local_context) |ctx| {
        ctx.split_shadow = self.shadow;
    }
}

pub fn closeShadowIndexManager(self: *DB) !void {
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    try self.closeShadowIndexManagerLocked();
}

pub fn closeShadowIndexManagerLocked(self: *DB) !void {
    try waitForSplitShadowDrainLocked(self, false);
    const shadow = self.shadow orelse return;
    if (self.transaction_recovery_local_context) |ctx| {
        ctx.split_shadow = null;
    }
    shadow.manager.deinit();
    self.alloc.destroy(shadow.manager);
    self.alloc.free(shadow.base_path);
    self.alloc.free(shadow.indexes_path);
    self.alloc.free(shadow.range_start);
    self.alloc.free(shadow.range_end);
    self.alloc.destroy(shadow);
    self.shadow = null;
}

pub fn getShadowIndexDir(self: *DB) []const u8 {
    const shadow = self.shadow orelse return "";
    return shadow.indexes_path;
}

pub fn split(
    self: *DB,
    curr_range: types.ByteRange,
    split_key: []const u8,
    dest_dir1: []const u8,
    dest_dir2: []const u8,
    prepare_only: bool,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, curr_range, split_key, dest_dir1, dest_dir2, prepare_only });
    if (self.local_execution.source_vectors.load(.acquire) != null) return error.VectorStoreLifecycleUnsupported;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    while (true) {
        const target_sequence = self.core.nextDerivedSequence();
        try self.runMaintenanceUntil(target_sequence, .{});

        try self.lockApplyForPortableRuntime();
        if (self.core.nextDerivedSequence() == target_sequence) break;
        self.core.unlockApply();
    }
    defer self.core.unlockApply();

    try self.requireSupportedRelationalTopology();
    if (!byteRangesEqual(self.core.byteRange(), curr_range)) return error.KeyOutOfRange;
    if (!curr_range.contains(split_key)) return error.KeyOutOfRange;
    // Shared FK guards and private metadata intents cannot move owners
    // midway through a durable decision. Drain them before range transfer.
    var transaction_manager = try self.core.initTxnManager();
    defer transaction_manager.deinit();
    if (try transaction_manager.hasSchemaLeases()) return error.SchemaInUse;

    const range2 = types.ByteRange{
        .start = split_key,
        .end = curr_range.end,
    };

    if (self.core.splitState() == null) {
        try self.core.prepareSplit(split_key);
    }

    try prepareSplitDestination(self, range2, dest_dir2);
    if (prepare_only) return;

    try finalizeSplitLocked(self, .{
        .start = curr_range.start,
        .end = split_key,
    });
}

pub fn finalizeSplit(self: *DB, new_range: types.ByteRange) !void {
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    try finalizeSplitLocked(self, new_range);
}

test "db split sim default workload stays green" {
    const alloc = std.testing.allocator;
    try runDbSplitReplayCase(alloc, "db-split-default", 0xA17F_E101, 8);
}

test "db split sim reopen-heavy workload stays green" {
    const alloc = std.testing.allocator;
    try runDbSplitReplayCase(alloc, "db-split-reopen-heavy", 0xA17F_E102, 10);
}

test "db split full keeps subsequent left-side source writes searchable" {
    const alloc = std.testing.allocator;

    var source_path_tmp = try TestDirectory.init("db");
    defer source_path_tmp.cleanup();
    var dest_path_tmp = try TestDirectory.init("db");
    defer dest_path_tmp.cleanup();
    const source_path = source_path_tmp.path().ptr;
    const dest_path = dest_path_tmp.path().ptr;
    defer cleanupTempDir(source_path);
    defer cleanupTempDir(dest_path);

    var runtime = try DbSplitSimRuntime.init(alloc, source_path, dest_path);
    defer runtime.deinit();

    try runtime.applyAction(.{ .add_doc = .left_alpha }, 0);
    try runtime.applyAction(.split_full, 1);
    try runtime.applyAction(.{ .add_doc = .left_alpha }, 2);

    try runtime.reopenSource();
    try runtime.source_db.?.runUntilIdle();
    try runtime.reopenDest();
    try runtime.dest_db.?.runUntilIdle();

    const raw = try runtime.source_db.?.get(alloc, "doc:b:2");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);

    const source_range = runtime.source_db.?.getRange();
    const source_applied = try runtime.source_db.?.core.loadAppliedSequence(alloc, db_split_sim_index_name);
    const source_replay_target = runtime.source_db.?.core.nextDerivedSequence();
    const summary = try runtime.summary(alloc);
    try std.testing.expectEqualStrings("", source_range.start);
    try std.testing.expectEqualStrings(db_split_sim_split_key, source_range.end);
    try std.testing.expectEqual(@as(u64, 2), source_replay_target);
    try std.testing.expectEqual(@as(u64, 2), source_applied);
    try std.testing.expectEqual(@as(u32, 2), summary.source_doc_count);
    try std.testing.expectEqual(@as(u32, 2), summary.source_alpha_hits);
    try std.testing.expectEqual(@as(u32, 0), summary.dest_doc_count);
}

test "db split replay fixtures stay green" {
    try runDbSplitReplayFixtures(std.testing.allocator);
}

test "db split modeled replay fixtures stay green" {
    try runModeledDbSplitReplayFixtures(std.testing.allocator);
}

test "db split modeled sim workloads stay green" {
    const alloc = std.testing.allocator;
    try runModeledDbSplitReplayCase(alloc, "db-split-modeled-default", 0xA17F_E301, 8);
    try runModeledDbSplitReplayCase(alloc, "db-split-modeled-reopen-heavy", 0xA17F_E302, 10);
}

test "db split state and split deltas are exposed through public api" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    var split_state = types.SplitState{
        .phase = .splitting,
        .split_key = try alloc.dupe(u8, "doc:m"),
        .new_shard_id = 42,
        .started_at = 1_000,
        .original_range_end = try alloc.dupe(u8, ""),
    };
    defer split_state.deinit(alloc);

    try db.setSplitState(split_state);

    const loaded_state = (try db.getSplitState(alloc)) orelse return error.TestExpectedEqual;
    defer {
        var state = loaded_state;
        state.deinit(alloc);
    }
    try std.testing.expectEqual(types.SplitPhase.splitting, loaded_state.phase);
    try std.testing.expectEqualStrings("doc:m", loaded_state.split_key);
    try std.testing.expectEqual(@as(u64, 42), loaded_state.new_shard_id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .deletes = &.{"doc:gone"},
        .timestamp_ns = 55_000,
    });

    try std.testing.expectEqual(@as(u64, 1), db.getSplitDeltaSeq());

    const entries = try db.listSplitDeltaEntriesAfter(alloc, 0);
    defer types.freeSplitDeltaEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 55_000), entries[0].timestamp);
    try std.testing.expect(entries[0].writes.len >= 4);
    try std.testing.expect(entries[0].deletes.len >= 2);

    try db.setSplitDeltaFinalSeq(9);
    try std.testing.expectEqual(@as(u64, 9), try db.getSplitDeltaFinalSeq(alloc));
    try db.clearSplitDeltaEntries();
    try std.testing.expectEqual(@as(u64, 0), db.getSplitDeltaSeq());
    try db.clearSplitDeltaFinalSeq();
    try std.testing.expectEqual(@as(u64, 0), try db.getSplitDeltaFinalSeq(alloc));
}

test "db split finalization marks split-off document child ranges remote" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
    });

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const moved_child_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:z", "document_units_v1", "page:000001");
    defer alloc.free(moved_child_key);

    const ChildRangeJson = struct {
        range_id: []const u8,
        range_kind: []const u8,
        artifact_name: []const u8,
        split_boundary: []const u8,
        placement: []const u8,
        owner_group_id: u64,
        placement_generation: u64,
        route_status: []const u8,
        split_eligible: bool,
        start_key: []const u8,
        end_key_exclusive: []const u8,
        last_key: []const u8,
        child_count: usize,
    };
    const ManifestJson = struct {
        manifest_version: u64,
        generation: u64,
        artifact_name: []const u8,
        source_url: []const u8,
        source_fingerprint: []const u8,
        content_type: []const u8,
        route_type: []const u8,
        child_ranges: []const ChildRangeJson,
    };
    const child_ranges = [_]ChildRangeJson{.{
        .range_id = "range:000000",
        .range_kind = "unit",
        .artifact_name = "document_units_v1",
        .split_boundary = "unit",
        .placement = "parent",
        .owner_group_id = 0,
        .placement_generation = 4,
        .route_status = "local_committed",
        .split_eligible = true,
        .start_key = moved_child_key,
        .end_key_exclusive = "",
        .last_key = moved_child_key,
        .child_count = 1,
    }};
    const manifest = try std.json.Stringify.valueAlloc(alloc, ManifestJson{
        .manifest_version = 2,
        .generation = 1,
        .artifact_name = "document_units_v1",
        .source_url = "data:text/plain;base64,YWxwaGE=",
        .source_fingerprint = "fp",
        .content_type = "text/plain",
        .route_type = "text",
        .child_ranges = child_ranges[0..],
    }, .{});
    defer alloc.free(manifest);
    const child_value = "{\"_parent_doc_key\":\"doc:a\",\"_artifact_name\":\"document_units_v1\",\"unit_id\":\"page:000001\",\"text\":\"moved\"}";
    try db.core.store.putBatch(&.{
        .{ .key = manifest_key, .value = manifest },
        .{ .key = moved_child_key, .value = child_value },
    }, &.{});

    try db.core.shard_manager.prepareSplit("doc:m");
    try db.core.shard_manager.split(7002, "doc:m");
    db.core.index_manager.updateRange(db.core.shard_manager.getByteRange());
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    const updated = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(updated);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"placement\":\"remote\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"owner_group_id\":7002") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"placement_generation\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"split_eligible\":false") != null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, moved_child_key));
}

test "db split prepare and finalize produce destination shard and trim parent range" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var dest_tmp = try TestDirectory.init("db");
    defer dest_tmp.cleanup();
    const dest = dest_tmp.path().ptr;

    const identity_namespace = doc_identity.Namespace{ .table_id = 71, .shard_id = 101, .range_id = 1001 };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .identity_namespace = identity_namespace,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
        },
        .graph_writes = &.{
            .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
        },
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(@as(?u64, 2), try range_cardinality.load(alloc, db.core.store));
    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);

    var split_db = try DB.open(alloc, std.mem.span(dest), .{});
    defer split_db.close();
    try std.testing.expectEqualStrings("doc:m", split_db.getRange().start);
    try std.testing.expectEqual(@as(?u64, 1), try range_cardinality.load(alloc, split_db.core.store));
    try std.testing.expect((try split_db.getSplitState(alloc)) == null);
    {
        const split_stats = try split_db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, split_stats);
        try std.testing.expectEqual(identity_namespace.table_id, split_stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(identity_namespace.shard_id, split_stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(identity_namespace.range_id, split_stats.doc_identity.namespace_range_id);
        try std.testing.expectEqual(@as(u32, 3), split_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), split_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), split_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), split_stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), split_stats.doc_identity.primary_docs_missing_identity_state);
    }

    const split_doc = (try split_db.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    var split_result = try split_db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer split_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_result.total_hits);

    var split_sparse = try split_db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer split_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", split_sparse.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 2), split_sparse.hits[0].doc_ordinal);

    var split_filtered = try split_db.search(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:z"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer split_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_filtered.total_hits);
    try std.testing.expectEqualStrings("doc:z", split_filtered.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 2), split_filtered.hits[0].doc_ordinal);

    var split_filtered_left_doc = try split_db.search(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:a"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer split_filtered_left_doc.deinit();
    try std.testing.expectEqual(@as(u32, 0), split_filtered_left_doc.total_hits);

    const split_incoming = try split_db.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, split_incoming);
    try std.testing.expectEqual(@as(usize, 1), split_incoming.len);
    try std.testing.expectEqualStrings("doc:z", split_incoming[0].source);

    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expectEqualStrings("doc:m", db.getRange().end);
    try std.testing.expectEqual(@as(?u64, 1), try range_cardinality.load(alloc, db.core.store));
    try std.testing.expect((try db.get(alloc, "doc:z")) == null);
    {
        const parent_stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, parent_stats);
        try std.testing.expectEqual(identity_namespace.table_id, parent_stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(identity_namespace.shard_id, parent_stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(identity_namespace.range_id, parent_stats.doc_identity.namespace_range_id);
        try std.testing.expectEqual(@as(u32, 3), parent_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), parent_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), parent_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), parent_stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), parent_stats.doc_identity.primary_docs_missing_identity_state);
    }

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);

    var parent_sparse = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer parent_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_sparse.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 1), parent_sparse.hits[0].doc_ordinal);

    var parent_filtered = try db.search(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:a"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer parent_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_filtered.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_filtered.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 1), parent_filtered.hits[0].doc_ordinal);

    var removed = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer removed.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed.total_hits);

    var removed_sparse = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer removed_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed_sparse.total_hits);

    const parent_incoming = try db.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, parent_incoming);
    try std.testing.expectEqual(@as(usize, 0), parent_incoming.len);
}

test "db split preserves ttl graph source fallback ownership on destination" {
    const alloc = std.testing.allocator;

    var parent_tmp = try TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;
    var child_tmp = try TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;

    var parent = try DB.open(alloc, std.mem.span(parent_path), .{});
    defer parent.close();
    try parent.addEnrichment(.{
        .name = "primary_relations_v1",
        .kind = .asset,
        .field = "primary_relations",
        .content_type = "application/json",
    });
    try parent.addEnrichment(.{
        .name = "fallback_relations_v1",
        .kind = .asset,
        .field = "fallback_relations",
        .content_type = "application/json",
    });
    try parent.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
        ,
    });
    try parent.batch(.{
        .writes = &.{.{
            .key = "doc:z",
            .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:y"},"weight":2,"winner":"primary"},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:y"},"weight":1,"winner":"fallback"}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try parent.runUntilIdle();
    try std.testing.expect((try parent.core.index_manager.graphIndex("relations_graph").?.index.nextContributionExpirationAtOrAfter(0)) != null);
    try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);

    var child = try DB.open(alloc, std.mem.span(child_path), .{});
    defer child.close();
    try std.testing.expect((try child.core.index_manager.graphIndex("relations_graph").?.index.nextContributionExpirationAtOrAfter(0)) != null);
    const child_due_before = try child.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, child_due_before);
    try std.testing.expectEqual(@as(usize, 2), child_due_before.len);
    try child.batch(.{
        .writes = &.{.{
            .key = "doc:z",
            .value =
            \\{"fallback_relations":{"type":"mentions","target":{"document_id":"doc:y"},"weight":1,"winner":"fallback"}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try child.runUntilIdle();
    const child_due_after = try child.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, child_due_after);
    try std.testing.expectEqual(@as(usize, 1), child_due_after.len);

    const child_edges = try child.getEdges(alloc, "relations_graph", "doc:z", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, child_edges);
    try std.testing.expectEqual(@as(usize, 1), child_edges.len);
    try std.testing.expectEqual(@as(f64, 1), child_edges[0].weight);
    try std.testing.expect(std.mem.indexOf(u8, child_edges[0].metadata, "\"winner\":\"fallback\"") != null);

    try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try parent.runUntilIdle();
    const parent_due = try parent.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, parent_due);
    try std.testing.expectEqual(@as(usize, 0), parent_due.len);
    try std.testing.expectEqual(@as(?u64, null), try parent.core.index_manager.graphIndex("relations_graph").?.index.nextContributionExpirationAtOrAfter(0));
    const parent_edges = try parent.getEdges(alloc, "relations_graph", "doc:z", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, parent_edges);
    try std.testing.expectEqual(@as(usize, 0), parent_edges.len);
}

test "db split retires moved graph owner ttl when source remains on parent" {
    const alloc = std.testing.allocator;
    var parent_tmp = try TestDirectory.init("db");
    defer parent_tmp.cleanup();
    var child_tmp = try TestDirectory.init("db");
    defer child_tmp.cleanup();
    var parent = try DB.open(alloc, parent_tmp.path(), .{});
    defer parent.close();
    try parent.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
    try parent.batch(.{
        .graph_writes = &.{.{
            .index_name = "graph_v1",
            .source = "doc:a",
            .owner = "doc:z",
            .target = "doc:t",
            .edge_type = "links",
            .weight = 1,
        }},
        .sync_level = .full_index,
    });
    const graph = &parent.core.index_manager.graphIndex("graph_v1").?.index;
    try std.testing.expect((try graph.nextContributionExpirationAtOrAfter(0)) != null);
    try parent.split(parent.getRange(), "doc:m", "", child_tmp.path(), true);
    db_zig.test_graph_split_retire_abort_before_cursor.store(true, .monotonic);
    defer db_zig.test_graph_split_retire_abort_before_cursor.store(false, .monotonic);
    try std.testing.expectError(error.TestInjectedBackfillFailure, parent.finalizeSplit(.{ .start = "", .end = "doc:m" }));
    try std.testing.expectError(error.NotFound, parent.core.store.get(alloc, graph_split_retire_cursor_key));
    const restored_edges = try parent.getEdges(alloc, "graph_v1", "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, restored_edges);
    try std.testing.expectEqual(@as(usize, 1), restored_edges.len);
    try std.testing.expect(graph.ttl_duration_ns > 0);
    try std.testing.expect((try graph.nextContributionExpirationAtOrAfter(0)) != null);
    try std.testing.expect(restored_edges[0].winner_key_hex.len > 0);
    try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try parent.runUntilIdle();
    try std.testing.expectEqual(@as(?u64, null), try graph.nextContributionExpirationAtOrAfter(0));
    const parent_edges = try parent.getEdges(alloc, "graph_v1", "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, parent_edges);
    try std.testing.expectEqual(@as(usize, 0), parent_edges.len);
    var child = try DB.open(alloc, child_tmp.path(), .{});
    defer child.close();
    try std.testing.expect((try child.core.index_manager.graphIndex("graph_v1").?.index.nextContributionExpirationAtOrAfter(0)) != null);
    const child_edges = try child.getEdges(alloc, "graph_v1", "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, child_edges);
    try std.testing.expectEqual(@as(usize, 1), child_edges.len);
}

test "db split retains ttl graph owner when physical source moves" {
    const alloc = std.testing.allocator;
    const Count = struct {
        fn privateDue(graph: *graph_mod.GraphIndex) !usize {
            const prefix = "meta:edge_expiration:v1:";
            var read = try graph.reverse_store.beginRead();
            defer read.abort();
            var cursor = try read.openCursor();
            defer cursor.close();
            var count: usize = 0;
            var row = try cursor.seekAtOrAfter(prefix);
            while (row) |entry| : (row = try cursor.next()) {
                if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                count += 1;
            }
            return count;
        }
    };
    var parent_tmp = try TestDirectory.init("db");
    defer parent_tmp.cleanup();
    var child_tmp = try TestDirectory.init("db");
    defer child_tmp.cleanup();
    var parent = try DB.open(alloc, parent_tmp.path(), .{});
    defer parent.close();
    try parent.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
    try parent.batch(.{
        .graph_writes = &.{
            .{
                .index_name = "graph_v1",
                .source = "doc:z",
                .owner = "doc:a",
                .target = "doc:t",
                .edge_type = "links",
                .weight = 1,
            },
            .{
                .index_name = "graph_v1",
                .source = "doc:z",
                .target = "doc:t",
                .edge_type = "links",
                .weight = 3,
            },
        },
        .sync_level = .full_index,
    });
    try parent.split(parent.getRange(), "doc:m", "", child_tmp.path(), true);
    var child = try DB.open(alloc, child_tmp.path(), .{});
    defer child.close();
    const direct_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:z", "graph_v1", "links", "doc:t");
    defer alloc.free(direct_key);
    const direct_artifact = try child.core.store.get(alloc, direct_key);
    defer alloc.free(direct_artifact);
    const child_edges = try child.getEdges(alloc, "graph_v1", "doc:z", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, child_edges);
    try std.testing.expectEqual(@as(usize, 1), child_edges.len);
    try std.testing.expectEqual(@as(f64, 3), child_edges[0].weight);
    try std.testing.expectEqual(@as(usize, 1), try Count.privateDue(&child.core.index_manager.graphIndex("graph_v1").?.index));
    try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    const immediate_edges = try parent.getEdges(alloc, "graph_v1", "doc:z", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, immediate_edges);
    try std.testing.expectEqual(@as(usize, 1), immediate_edges.len);
    try parent.runUntilIdle();
    try std.testing.expect((try parent.core.index_manager.graphIndex("graph_v1").?.index.nextContributionExpirationAtOrAfter(0)) != null);
    try std.testing.expectEqual(@as(usize, 1), try Count.privateDue(&parent.core.index_manager.graphIndex("graph_v1").?.index));
    const parent_edges = try parent.getEdges(alloc, "graph_v1", "doc:z", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, parent_edges);
    try std.testing.expectEqual(@as(usize, 1), parent_edges.len);
    try std.testing.expectEqual(@as(f64, 1), parent_edges[0].weight);
    try parent.batch(.{
        .graph_writes = &.{.{
            .index_name = "graph_v1",
            .source = "doc:z",
            .owner = "doc:a",
            .target = "doc:t",
            .edge_type = "links",
            .weight = 2,
        }},
        .sync_level = .full_index,
    });
    const updated = try parent.getEdges(alloc, "graph_v1", "doc:z", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, updated);
    try std.testing.expectEqual(@as(usize, 1), updated.len);
    try std.testing.expectEqual(@as(f64, 2), updated[0].weight);
}

test "db split prepare and finalize work with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var dest_tmp = try TestDirectory.init("db");
    defer dest_tmp.cleanup();
    const dest = dest_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
    });
    defer db.close();

    const schema_v1 = "{\"version\":1,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true}}}}";
    const schema_v2 = "{\"version\":2,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true}}}}";
    try db.setSchemaJson(alloc, schema_v1);
    try db.setSchemaJson(alloc, schema_v2);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
    });

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);

    var split_db = try DB.open(alloc, std.mem.span(dest), .{
        .primary_backend = primary_backend,
    });
    defer split_db.close();
    const copied_schema = (try split_db.getSchemaJson(alloc)) orelse return error.TestUnexpectedResult;
    defer alloc.free(copied_schema);
    try std.testing.expectEqualStrings(schema_v2, copied_schema);
    const old_schema_key = try public_table_schema.versionedSchemaKeyAlloc(alloc, 1);
    defer alloc.free(old_schema_key);
    const copied_old_schema = try split_db.core.store.get(alloc, old_schema_key);
    defer alloc.free(copied_old_schema);
    try std.testing.expectEqualStrings(schema_v1, copied_old_schema);
    try std.testing.expectEqualStrings("doc:m", split_db.getRange().start);
    const copied_admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, "ft_v1");
    defer alloc.free(copied_admission_key);
    try std.testing.expectError(error.NotFound, split_db.core.store.get(alloc, copied_admission_key));
    try std.testing.expect(!split_db.core.index_manager.repairUnavailable("ft_v1"));
    {
        const split_stats = try split_db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, split_stats);
        try std.testing.expectEqual(@as(u32, 3), split_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), split_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), split_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), split_stats.doc_identity.primary_docs_missing_ordinals);
    }

    const split_doc = (try split_db.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    var split_result = try split_db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer split_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_result.total_hits);

    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expectEqualStrings("doc:m", db.getRange().end);
    try std.testing.expect((try db.get(alloc, "doc:z")) == null);
    {
        const parent_stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, parent_stats);
        try std.testing.expectEqual(@as(u32, 3), parent_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), parent_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), parent_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), parent_stats.doc_identity.primary_docs_missing_ordinals);
    }

    var removed = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer removed.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed.total_hits);
}

test "db split prepare survives reopen and finalizes with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var parent_tmp = try TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;

    var child_tmp = try TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .primary_backend = primary_backend,
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
                .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
            },
        });

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.sync(true);
    }

    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_parent.close();

    const split_state = (try reopened_parent.getSplitState(alloc)) orelse return error.TestExpectedEqual;
    defer types.freeSplitState(alloc, split_state);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);

    var child = try DB.open(alloc, std.mem.span(child_path), .{
        .primary_backend = primary_backend,
    });
    defer child.close();

    const split_doc = (try child.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    try reopened_parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expect((try reopened_parent.get(alloc, "doc:z")) == null);
}

test "db split prepare survives reopen and finalizes text sparse and graph indexes with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var parent_tmp = try TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;

    var child_tmp = try TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .primary_backend = primary_backend,
        });
        defer parent.close();

        const index_cfgs = [_]types.IndexConfig{
            .{
                .name = "ft_v1",
                .kind = .full_text,
                .config_json = "{\"field\":\"title\"}",
            },
            .{
                .name = "sp_v1",
                .kind = .sparse_vector,
                .config_json = "{\"field\":\"sparse\"}",
            },
            .{
                .name = "graph_v1",
                .kind = .graph,
                .config_json = "{}",
            },
        };
        for (index_cfgs) |cfg| try parent.addIndex(cfg);

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
                .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
            },
            .graph_writes = &.{
                .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
            },
            .sync_level = .full_index,
        });

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.sync(true);
    }

    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_parent.close();

    const split_state = (try reopened_parent.getSplitState(alloc)) orelse return error.TestExpectedEqual;
    defer types.freeSplitState(alloc, split_state);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);

    var child = try DB.open(alloc, std.mem.span(child_path), .{
        .primary_backend = primary_backend,
    });
    defer child.close();

    const split_doc = (try child.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    var child_text = try child.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer child_text.deinit();
    try std.testing.expectEqual(@as(u32, 1), child_text.total_hits);
    try std.testing.expectEqualStrings("doc:z", child_text.hits[0].id);

    var child_sparse = try child.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer child_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), child_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", child_sparse.hits[0].id);

    const child_incoming = try child.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, child_incoming);
    try std.testing.expectEqual(@as(usize, 1), child_incoming.len);
    try std.testing.expectEqualStrings("doc:z", child_incoming[0].source);

    try reopened_parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expect((try reopened_parent.get(alloc, "doc:z")) == null);

    var parent_text = try reopened_parent.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer parent_text.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_text.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_text.hits[0].id);

    var removed_text = try reopened_parent.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer removed_text.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed_text.total_hits);

    var parent_sparse = try reopened_parent.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer parent_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_sparse.hits[0].id);

    var removed_sparse = try reopened_parent.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer removed_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed_sparse.total_hits);

    const parent_incoming = try reopened_parent.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, parent_incoming);
    try std.testing.expectEqual(@as(usize, 0), parent_incoming.len);
}

test "db split cutover fences enrichment to the owning range" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var dest_tmp = try TestDirectory.init("db");
    defer dest_tmp.cleanup();
    const dest = dest_tmp.path().ptr;

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
            .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
        },
        .sync_level = .write,
    });
    try db.enrichment_runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent should reject\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var child = try DB.open(alloc, std.mem.span(dest), .{
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer child.close();

    const child_before = try child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);
    const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");

    try child.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child owns this\"}" }},
        .sync_level = .write,
    });
    try child.runUntilIdle();
    const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(child_dense_after >= child_dense_before + 2);

    const query_vec = try deterministic_child.interface().embedDense(alloc, "", "child owns this", 3);
    defer alloc.free(query_vec);

    var child_result = try child.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer child_result.deinit();
    try std.testing.expect(child_result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:y", child_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
    });
    defer parent_result.deinit();
    for (parent_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:y"));
    }
}

test "db split bootstrap replacement preserves overlapping incoming documents" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;
    try db.batch(.{ .writes = &.{
        .{ .key = "doc:keep", .value = "{\"version\":1}" },
        .{ .key = "doc:stale", .value = "{\"stale\":true}" },
    } });

    try std.testing.expect(try db.replaceSplitBootstrap(
        alloc,
        .{ .start = "", .end = "" },
        &.{
            .{ .key = "doc:keep", .value = "{\"version\":2}" },
            .{ .key = "doc:new", .value = "{\"new\":true}" },
        },
        17,
        .{
            .transition_id = 11,
            .attempt_epoch = 1,
            .source_group_id = 101,
            .destination_group_id = 102,
            .bootstrap_complete = false,
        },
    ));

    const retained = (try db.get(alloc, "doc:keep")).?;
    defer alloc.free(retained);
    try std.testing.expectEqualStrings("{\"version\":2}", retained);
    const added = (try db.get(alloc, "doc:new")).?;
    defer alloc.free(added);
    try std.testing.expectEqualStrings("{\"new\":true}", added);
    try std.testing.expect((try db.get(alloc, "doc:stale")) == null);
}

test "db split bootstrap retries preserve reserved data and exact sequence" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;
    const byte_range: types.ByteRange = .{ .start = "doc:m", .end = "doc:z" };
    const incomplete: range_state_mod.SplitBootstrapMarker = .{
        .transition_id = 11,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 102,
        .bootstrap_complete = false,
    };
    try std.testing.expect(try db.replaceSplitBootstrap(
        alloc,
        byte_range,
        &.{.{ .key = "doc:m", .value = "{\"version\":1}" }},
        17,
        incomplete,
    ));
    try std.testing.expect(!try db.replaceSplitBootstrap(alloc, byte_range, &.{}, 17, incomplete));
    const reserved = (try db.get(alloc, "doc:m")).?;
    defer alloc.free(reserved);
    try std.testing.expectEqualStrings("{\"version\":1}", reserved);

    var complete = incomplete;
    complete.bootstrap_complete = true;
    try std.testing.expectError(
        error.StaleSplitBootstrap,
        db.completeSplitBootstrap(alloc, byte_range, 18, complete),
    );
    try std.testing.expect(try db.completeSplitBootstrap(alloc, byte_range, 17, complete));

    try std.testing.expect(!try db.replaceSplitBootstrap(alloc, byte_range, &.{}, 17, incomplete));
    const retained = (try db.get(alloc, "doc:m")).?;
    defer alloc.free(retained);
    try std.testing.expectEqualStrings("{\"version\":1}", retained);
    try std.testing.expect((try db.getSplitBootstrapMarker(alloc)).?.bootstrap_complete);

    try std.testing.expectError(
        error.SplitBootstrapComplete,
        db.replaceSplitBootstrap(alloc, byte_range, &.{}, 18, incomplete),
    );
}

test "db split cutover fences enrichment to the owning range with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var dest_tmp = try TestDirectory.init("db");
    defer dest_tmp.cleanup();
    const dest = dest_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
            .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
        },
        .sync_level = .write,
    });
    try db.enrichment_runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent should reject\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var child = try DB.open(alloc, std.mem.span(dest), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer child.close();

    const child_before = try child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);
    const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");

    try child.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child owns this\"}" }},
        .sync_level = .write,
    });
    try child.runUntilIdle();
    const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(child_dense_after >= child_dense_before + 2);

    const query_vec = try deterministic_child.interface().embedDense(alloc, "", "child owns this", 3);
    defer alloc.free(query_vec);

    var child_result = try child.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer child_result.deinit();
    try std.testing.expect(child_result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:y", child_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
    });
    defer parent_result.deinit();
    for (parent_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:y"));
    }
}

test "db split cutover preserves enrichment resume and fencing across reopen" {
    const alloc = std.testing.allocator;

    var parent_tmp = try TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;

    var child_tmp = try TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;

    {
        var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .enrichment = .{
                .owner_id = "split-parent",
                .dense_embedder = deterministic_parent.interface(),
            },
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
                .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
            },
            .sync_level = .write,
        });
        try parent.enrichment_runtime.?.waitForApplied(1);
        try waitForDerivedReplayTarget(&parent);

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });

        var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
        var child = try DB.open(alloc, std.mem.span(child_path), .{
            .enrichment = .{
                .owner_id = "split-child",
                .dense_embedder = deterministic_child.interface(),
            },
        });
        defer child.close();

        const child_before = try child.stats(alloc);
        defer types.freeDBStats(alloc, child_before);
        const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try child.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child pre-reopen\"}" }},
            .sync_level = .write,
        });
        try child.runUntilIdle();
        const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expect(child_dense_after > child_dense_before);
    }

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer reopened_parent.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_parent.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_child = try DB.open(alloc, std.mem.span(child_path), .{
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer reopened_child.close();

    const child_before = try reopened_child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);

    try reopened_child.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"child post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_child.runUntilIdle();

    const pre_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:y")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const post_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child post-reopen", 3);
    defer alloc.free(post_reopen_vec);

    var child_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = post_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer child_result.deinit();
    var found_post_reopen = false;
    for (child_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:x")) found_post_reopen = true;
    }
    try std.testing.expect(found_post_reopen);
}

test "db split cutover preserves enrichment resume and fencing across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var parent_tmp = try TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;

    var child_tmp = try TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "split-parent",
                .dense_embedder = deterministic_parent.interface(),
            },
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
                .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
            },
            .sync_level = .write,
        });
        try parent.enrichment_runtime.?.waitForApplied(1);
        try waitForDerivedReplayTarget(&parent);

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });

        var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
        var child = try DB.open(alloc, std.mem.span(child_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "split-child",
                .dense_embedder = deterministic_child.interface(),
            },
        });
        defer child.close();

        const child_before = try child.stats(alloc);
        defer types.freeDBStats(alloc, child_before);
        const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try child.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child pre-reopen\"}" }},
            .sync_level = .write,
        });
        try child.runUntilIdle();
        const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expect(child_dense_after > child_dense_before);
        try parent.sync(true);
        try child.sync(true);
    }

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer reopened_parent.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_parent.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_child = try DB.open(alloc, std.mem.span(child_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer reopened_child.close();

    const child_before = try reopened_child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);

    try reopened_child.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"child post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_child.runUntilIdle();

    const pre_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:y")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const post_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child post-reopen", 3);
    defer alloc.free(post_reopen_vec);

    var child_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = post_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer child_result.deinit();
    var found_post_reopen = false;
    for (child_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:x")) found_post_reopen = true;
    }
    try std.testing.expect(found_post_reopen);
}
