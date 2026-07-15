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
const raft_engine = @import("raft_engine");
const fs_paths = @import("../../common/fs_paths.zig");
const docstore = @import("../../storage/docstore.zig");
const lsm_backend = @import("../../storage/lsm_backend.zig");
const raft_storage_mod = @import("../../raft/storage/mod.zig");
const wal_replica_state_mod = @import("../../raft/storage/wal_replica_state.zig");
const shard_mod = @import("../../storage/shard.zig");
const raft_state_machine = @import("../../raft/state_machine/mod.zig");
const shard_state_store = @import("shard_state_store.zig");
const data_raft_batch = @import("../raft_batch.zig");
const batch_shard_count: usize = 64;

pub const AppliedDataBatch = struct {
    commit_index: u64,
    entry_count: usize,
    normal_entry_count: usize,
    admin_entry_count: usize,
    last_entry_term: u64,
    last_entry_index: u64,
};

pub const AppliedNormalEntry = struct {
    index: u64,
    data: []const u8,
};

pub const AppliedDataKV = shard_state_store.AppliedDataKV;
pub const AppliedDataRange = shard_state_store.AppliedDataRange;
pub const AppliedSplitState = shard_state_store.AppliedSplitState;
pub const SplitHandoff = shard_state_store.SplitHandoff;

pub const RaftApplyStoreConfig = struct {
    root_dir: []const u8,
    map_size: usize = 64 * 1024 * 1024,
    no_sync: bool = false,
    /// Writable stores inherit the LSM backend's process and native-path
    /// exclusive writer lease. Read-only inspection may coexist with the owner.
    read_only: bool = false,
};

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    path: []u8,
    backend: lsm_backend.BackendHandle,
    store: docstore.DocStore,
    batch_shards: [batch_shard_count]BatchShard = [_]BatchShard{.{}} ** batch_shard_count,

    const OwnedBatch = AppliedDataBatch;
    const BatchShard = struct {
        mutex: std.Io.Mutex = .init,
        batches: std.AutoHashMapUnmanaged(u64, OwnedBatch) = .empty,
    };

    pub fn init(alloc: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();

        const root_dir = try alloc.dupe(u8, cfg.root_dir);
        errdefer alloc.free(root_dir);

        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), root_dir);

        const path = try std.fmt.allocPrint(alloc, "{s}/data-apply-store", .{root_dir});
        errdefer alloc.free(path);
        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), path);

        var backend = try lsm_backend.BackendHandle.open(alloc, path, .{
            .backend = .{
                .durability = if (cfg.no_sync) .none else .full,
                .read_only = cfg.read_only,
                .create_if_missing = !cfg.read_only,
            },
            .flush_threshold = 1,
        });
        errdefer backend.close();

        var runtime_store = try backend.backend.runtimeStore(alloc, .{ .name = "data-apply" });
        errdefer runtime_store.deinit();

        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .root_dir = root_dir,
            .path = path,
            .backend = backend,
            .store = try docstore.DocStore.openRuntime(alloc, runtime_store),
        };
    }

    pub fn deinit(self: *RaftApplyStore) void {
        for (&self.batch_shards) |*shard| shard.batches.deinit(self.alloc);
        self.store.close();
        self.backend.close();
        self.alloc.free(self.path);
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn snapshotBuilder(self: *RaftApplyStore) raft_state_machine.SnapshotBuilder {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_snapshot = buildSnapshot,
                .apply_batch = applyBatch,
            },
        };
    }

    pub fn latestBatch(self: *RaftApplyStore, group_id: u64) !?AppliedDataBatch {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const batch = (try self.ensureLoaded(shard, group_id)) orelse return null;
        return batch.*;
    }

    pub fn appliedNormalEntries(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]AppliedNormalEntry {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try normalEntryPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        var entries = try alloc.alloc(AppliedNormalEntry, kvs.len);
        errdefer {
            for (entries[0..kvs.len]) |entry| alloc.free(entry.data);
            alloc.free(entries);
        }
        for (kvs, 0..) |kv, i| {
            entries[i] = .{
                .index = try parseNormalEntryIndex(kv.key, prefix.len),
                .data = try alloc.dupe(u8, kv.value),
            };
        }
        return entries;
    }

    pub fn groupState(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]AppliedDataKV {
        return try shard_state_store.groupState(&self.store, alloc, group_id);
    }

    /// Seeds a group that predates the data-Raft apply projection. Index zero is
    /// reserved for this synthetic baseline so later real Raft entries retain
    /// their original indexes. The per-group apply shard lock makes the
    /// existence check atomic with normal apply. Snapshot data and its baseline
    /// watermark are committed in one DocStore batch for crash-safe retry.
    pub fn seedGroupSnapshotIfAbsent(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        byte_range: AppliedDataRange,
        entries: []const AppliedDataKV,
    ) !bool {
        const encoded = try raft_state_machine.encodeCommittedEntries(alloc, &.{.{
            .term = 0,
            .index = 0,
            .entry_type = .normal,
            .data = @constCast("snapshot_seed"),
        }});
        defer alloc.free(encoded);

        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        if ((try self.ensureLoaded(shard, group_id)) != null) return false;
        try shard.batches.ensureUnusedCapacity(self.alloc, 1);

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const value = try alloc.alloc(u8, @sizeOf(u64) + encoded.len);
        defer alloc.free(value);
        std.mem.writeInt(u64, value[0..8], 0, .little);
        @memcpy(value[8..], encoded);
        try shard_state_store.replaceGroupSnapshotWithMetadata(
            &self.store,
            alloc,
            group_id,
            byte_range,
            entries,
            &.{.{ .key = key, .value = value }},
        );

        var summary = try summarizeEntries(alloc, encoded);
        summary.commit_index = 0;
        shard.batches.putAssumeCapacity(group_id, summary);
        return true;
    }

    pub fn currentRange(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !AppliedDataRange {
        return try shard_state_store.currentRange(&self.store, alloc, group_id);
    }

    pub fn currentSplitState(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?AppliedSplitState {
        return try shard_state_store.currentSplitState(&self.store, alloc, group_id);
    }

    pub fn currentSplitDeltaSequence(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !u64 {
        return try shard_state_store.currentSplitDeltaSequence(&self.store, alloc, group_id);
    }

    pub fn currentSplitAcknowledgement(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?shard_state_store.SplitAcknowledgement {
        return try shard_state_store.currentSplitAcknowledgement(&self.store, alloc, group_id);
    }

    pub fn captureSplitHandoff(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !SplitHandoff {
        return try shard_state_store.captureSplitHandoff(&self.store, alloc, group_id);
    }

    pub fn listSplitDeltasAfter(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, after_seq: u64) ![]shard_state_store.SplitDelta {
        return try shard_state_store.listDeltasAfter(&self.store, alloc, group_id, after_seq);
    }

    pub fn applySplitHandoff(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, handoff: SplitHandoff) !void {
        try shard_state_store.applyHandoff(&self.store, alloc, group_id, handoff);
    }

    pub fn applySplitDeltas(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, deltas: []const shard_state_store.SplitDelta) !void {
        try shard_state_store.applyDeltas(&self.store, alloc, group_id, deltas);
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        return try shard_state_store.buildSnapshot(&self.store, alloc, group_id);
    }

    fn applyBatch(ptr: *anyopaque, batch: raft_state_machine.ApplyBatch) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try self.writeBatch(batch.group_id, batch.commit_index, batch.entries_bytes);
    }

    fn writeBatch(self: *RaftApplyStore, group_id: u64, commit_index: u64, entries_bytes: []const u8) !void {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.writeBatchLocked(shard, group_id, commit_index, entries_bytes);
    }

    fn writeBatchLocked(
        self: *RaftApplyStore,
        shard: *BatchShard,
        group_id: u64,
        commit_index: u64,
        entries_bytes: []const u8,
    ) !void {
        const existing_batch = try self.ensureLoaded(shard, group_id);
        if (existing_batch) |existing| {
            if (commit_index < existing.commit_index) return error.OutOfOrderDataApplyBatch;
            if (commit_index == existing.commit_index) {
                try self.verifyPersistedBatch(group_id, commit_index, entries_bytes);
                return;
            }
        } else {
            // Reserve before the durable write so cache publication cannot fail
            // after storage has advanced.
            try shard.batches.ensureUnusedCapacity(self.alloc, 1);
        }
        // A failed sibling state machine can leave this store ahead of Raft's
        // shared applied watermark. Raft then legitimately presents an
        // overlapping committed prefix. Apply effects only for entries newer
        // than this store's own durable watermark.
        const metadata = try describeEntries(
            self.alloc,
            entries_bytes,
            if (existing_batch) |existing| existing.last_entry_index else 0,
        );
        defer {
            for (metadata.normal_entries) |entry| self.alloc.free(entry.data);
            self.alloc.free(metadata.normal_entries);
            for (metadata.operations) |op| switch (op) {
                .put => |put| {
                    self.alloc.free(put.key);
                    self.alloc.free(put.value);
                },
                .delete => |key_to_delete| self.alloc.free(key_to_delete),
                .set_range => |range| {
                    self.alloc.free(range.start);
                    self.alloc.free(range.end);
                },
                .prepare_split, .start_split, .finalize_split, .rollback_split => |transition| self.alloc.free(transition.split_key),
                .acknowledge_split => {},
            };
            self.alloc.free(metadata.operations);
        }
        var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
        defer shard_state_store.freeOwnedWrites(self.alloc, &writes);
        var deletes = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (deletes.items) |key_to_delete| self.alloc.free(key_to_delete);
            deletes.deinit(self.alloc);
        }
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        {
            const owned_key = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(owned_key);
            const value = try self.alloc.alloc(u8, @sizeOf(u64) + entries_bytes.len);
            errdefer self.alloc.free(value);
            std.mem.writeInt(u64, value[0..8], commit_index, .little);
            @memcpy(value[8..], entries_bytes);
            try writes.append(self.alloc, .{ .key = owned_key, .value = value });
        }

        for (metadata.normal_entries) |entry| {
            var normal_key_buf: [160]u8 = undefined;
            const normal_key = try normalEntryKeyForGroup(&normal_key_buf, group_id, entry.index);
            const owned_key = try self.alloc.dupe(u8, normal_key);
            errdefer self.alloc.free(owned_key);
            const owned_value = try self.alloc.dupe(u8, entry.data);
            errdefer self.alloc.free(owned_value);
            try writes.append(self.alloc, .{ .key = owned_key, .value = owned_value });
        }
        try shard_state_store.appendOperationEffects(&self.store, self.alloc, group_id, metadata.operations, &writes, &deletes);
        try shard_state_store.putOwnedBatch(&self.store, self.alloc, writes.items, deletes.items);

        const summary = AppliedDataBatch{
            .commit_index = commit_index,
            .entry_count = metadata.entry_count,
            .normal_entry_count = metadata.normal_entry_count,
            .admin_entry_count = metadata.admin_entry_count,
            .last_entry_term = metadata.last_entry_term,
            .last_entry_index = metadata.last_entry_index,
        };
        if (shard.batches.getPtr(group_id)) |existing| {
            existing.* = summary;
            return;
        }
        shard.batches.putAssumeCapacity(group_id, summary);
    }

    fn verifyPersistedBatch(self: *RaftApplyStore, group_id: u64, commit_index: u64, entries_bytes: []const u8) !void {
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return error.InvalidDataApplyBatch,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidDataApplyBatch;
        if (std.mem.readInt(u64, encoded[0..8], .little) != commit_index or
            !std.mem.eql(u8, encoded[8..], entries_bytes))
        {
            return error.ConflictingDataApplyBatch;
        }
    }

    fn batchShard(self: *RaftApplyStore, group_id: u64) *BatchShard {
        return &self.batch_shards[@as(usize, @intCast(group_id % batch_shard_count))];
    }

    fn ensureLoaded(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) !?*OwnedBatch {
        if (shard.batches.getPtr(group_id)) |batch| return batch;

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidDataApplyBatch;

        const commit_index = std.mem.readInt(u64, encoded[0..8], .little);
        var summary = try summarizeEntries(self.alloc, encoded[8..]);
        summary.commit_index = commit_index;
        try shard.batches.put(self.alloc, group_id, summary);
        return shard.batches.getPtr(group_id);
    }

    fn summarizeEntries(alloc: std.mem.Allocator, entries_bytes: []const u8) !AppliedDataBatch {
        const decoded = try raft_state_machine.decodeCommittedEntries(alloc, entries_bytes);
        defer alloc.free(decoded);
        var normal_entry_count: usize = 0;
        var admin_entry_count: usize = 0;
        for (decoded) |entry| switch (entry.entry_type) {
            .normal => normal_entry_count += 1,
            .conf_change, .conf_change_v2 => admin_entry_count += 1,
        };
        return .{
            .commit_index = 0,
            .entry_count = decoded.len,
            .normal_entry_count = normal_entry_count,
            .admin_entry_count = admin_entry_count,
            .last_entry_term = if (decoded.len > 0) decoded[decoded.len - 1].term else 0,
            .last_entry_index = if (decoded.len > 0) decoded[decoded.len - 1].index else 0,
        };
    }

    const EntryMetadata = struct {
        entry_count: usize,
        normal_entry_count: usize,
        admin_entry_count: usize,
        last_entry_term: u64,
        last_entry_index: u64,
        normal_entries: []AppliedNormalEntry,
        operations: []DataOperation,
    };

    const DataOperation = shard_state_store.DataOperation;

    fn describeEntries(alloc: std.mem.Allocator, entries_bytes: []const u8, after_index: u64) !EntryMetadata {
        const decoded = try raft_state_machine.decodeCommittedEntries(alloc, entries_bytes);
        defer alloc.free(decoded);
        var normal_entry_count: usize = 0;
        var admin_entry_count: usize = 0;
        var normal_entries = std.ArrayListUnmanaged(AppliedNormalEntry).empty;
        var operations = std.ArrayListUnmanaged(DataOperation).empty;
        errdefer {
            for (normal_entries.items) |entry| alloc.free(entry.data);
            normal_entries.deinit(alloc);
        }
        errdefer {
            for (operations.items) |op| switch (op) {
                .put => |put| {
                    alloc.free(put.key);
                    alloc.free(put.value);
                },
                .delete => |key_to_delete| alloc.free(key_to_delete),
                .set_range => |range| {
                    alloc.free(range.start);
                    alloc.free(range.end);
                },
                .prepare_split, .start_split, .finalize_split, .rollback_split => |transition| alloc.free(transition.split_key),
                .acknowledge_split => {},
            };
            operations.deinit(alloc);
        }
        for (decoded) |entry| {
            switch (entry.entry_type) {
                .normal => {
                    normal_entry_count += 1;
                    if (entry.index <= after_index) continue;
                    try normal_entries.append(alloc, .{
                        .index = entry.index,
                        .data = try alloc.dupe(u8, entry.data),
                    });
                    try appendDataOperations(alloc, entry.data, &operations);
                },
                .conf_change, .conf_change_v2 => admin_entry_count += 1,
            }
        }
        if (decoded.len == 0) {
            return .{
                .entry_count = 0,
                .normal_entry_count = 0,
                .admin_entry_count = 0,
                .last_entry_term = 0,
                .last_entry_index = 0,
                .normal_entries = try normal_entries.toOwnedSlice(alloc),
                .operations = try operations.toOwnedSlice(alloc),
            };
        }
        const last = decoded[decoded.len - 1];
        return .{
            .entry_count = decoded.len,
            .normal_entry_count = normal_entry_count,
            .admin_entry_count = admin_entry_count,
            .last_entry_term = last.term,
            .last_entry_index = last.index,
            .normal_entries = try normal_entries.toOwnedSlice(alloc),
            .operations = try operations.toOwnedSlice(alloc),
        };
    }

    fn keyForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_raft_apply:{d}", .{group_id});
    }

    fn normalEntryPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_raft_normal:{d}:", .{group_id});
    }

    fn normalEntryKeyForGroup(buf: []u8, group_id: u64, index: u64) ![]const u8 {
        const prefix = try normalEntryPrefixForGroup(buf[0 .. buf.len - 8], group_id);
        const suffix: *[8]u8 = @ptrCast(buf[prefix.len .. prefix.len + 8]);
        std.mem.writeInt(u64, suffix, index, .big);
        return buf[0 .. prefix.len + 8];
    }

    fn parseNormalEntryIndex(key: []const u8, prefix_len: usize) !u64 {
        if (key.len != prefix_len + 8) return error.InvalidAppliedNormalEntryKey;
        return std.mem.readInt(u64, key[prefix_len..][0..8], .big);
    }

    fn parseSplitTransition(alloc: std.mem.Allocator, payload: []const u8) !shard_state_store.SplitTransition {
        const sep = std.mem.indexOfScalar(u8, payload, ':') orelse return error.InvalidAppliedDataRange;
        return .{
            .new_shard_id = try std.fmt.parseInt(u64, payload[0..sep], 10),
            .split_key = try alloc.dupe(u8, payload[sep + 1 ..]),
        };
    }

    fn parseDataOperation(alloc: std.mem.Allocator, data: []const u8) !?DataOperation {
        if (std.mem.startsWith(u8, data, "range:")) {
            const payload = data["range:".len..];
            if (std.mem.indexOfScalar(u8, payload, ':')) |first_sep| {
                if (first_sep > 0) {
                    const namespace = payload[0 .. first_sep + 1];
                    if (std.mem.indexOfPos(u8, payload, namespace.len, namespace)) |repeat_pos| {
                        const sep = repeat_pos - 1;
                        return .{ .set_range = .{
                            .start = try alloc.dupe(u8, payload[0..sep]),
                            .end = try alloc.dupe(u8, payload[repeat_pos..]),
                        } };
                    }
                }
            }
            if (std.mem.indexOfScalar(u8, payload, ':')) |sep| {
                return .{ .set_range = .{
                    .start = try alloc.dupe(u8, payload[0..sep]),
                    .end = try alloc.dupe(u8, payload[sep + 1 ..]),
                } };
            }
            return error.InvalidAppliedDataRange;
        }
        if (std.mem.startsWith(u8, data, "split_prepare:")) {
            return .{ .prepare_split = try parseSplitTransition(alloc, data["split_prepare:".len..]) };
        }
        if (std.mem.startsWith(u8, data, "split_start:")) {
            return .{ .start_split = try parseSplitTransition(alloc, data["split_start:".len..]) };
        }
        if (std.mem.startsWith(u8, data, "split_finalize:") or std.mem.startsWith(u8, data, "finalize_split:")) {
            const prefix_len = if (std.mem.startsWith(u8, data, "split_finalize:")) "split_finalize:".len else "finalize_split:".len;
            return .{ .finalize_split = try parseSplitTransition(alloc, data[prefix_len..]) };
        }
        if (std.mem.startsWith(u8, data, "split_rollback:") or std.mem.startsWith(u8, data, "rollback_split:")) {
            const prefix_len = if (std.mem.startsWith(u8, data, "split_rollback:")) "split_rollback:".len else "rollback_split:".len;
            return .{ .rollback_split = try parseSplitTransition(alloc, data[prefix_len..]) };
        }
        if (std.mem.startsWith(u8, data, "put:")) {
            const payload = data["put:".len..];
            if (std.mem.indexOfScalar(u8, payload, '=')) |sep| {
                return .{ .put = .{
                    .key = try alloc.dupe(u8, payload[0..sep]),
                    .value = try alloc.dupe(u8, payload[sep + 1 ..]),
                } };
            }
            return .{ .put = .{
                .key = try alloc.dupe(u8, payload),
                .value = try alloc.dupe(u8, ""),
            } };
        }
        if (std.mem.startsWith(u8, data, "del:")) {
            return .{ .delete = try alloc.dupe(u8, data["del:".len..]) };
        }
        return null;
    }

    fn appendDataOperations(
        alloc: std.mem.Allocator,
        data: []const u8,
        operations: *std.ArrayListUnmanaged(DataOperation),
    ) !void {
        if (!data_raft_batch.looksLikeEnvelope(data)) {
            if (try parseDataOperation(alloc, data)) |op| try operations.append(alloc, op);
            return;
        }

        var decoded = try data_raft_batch.decode(alloc, data);
        defer decoded.deinit(alloc);
        for (decoded.batch.req.writes) |write| {
            const key = try alloc.dupe(u8, write.key);
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, write.value);
            errdefer alloc.free(value);
            try operations.append(alloc, .{ .put = .{ .key = key, .value = value } });
        }
        for (decoded.batch.req.deletes) |key| {
            const owned_key = try alloc.dupe(u8, key);
            errdefer alloc.free(owned_key);
            try operations.append(alloc, .{ .delete = owned_key });
        }
        if (decoded.batch.req.split_transition) |transition| switch (transition.kind) {
            .prepare => {
                const split_key = try alloc.dupe(u8, transition.split_key);
                errdefer alloc.free(split_key);
                try operations.append(alloc, .{ .prepare_split = .{
                    .new_shard_id = transition.destination_group_id,
                    .split_key = split_key,
                } });
            },
            .start => {
                const split_key = try alloc.dupe(u8, transition.split_key);
                errdefer alloc.free(split_key);
                try operations.append(alloc, .{ .start_split = .{
                    .new_shard_id = transition.destination_group_id,
                    .split_key = split_key,
                } });
            },
            .finalize, .rollback => {
                const split_key = try alloc.dupe(u8, transition.split_key);
                errdefer alloc.free(split_key);
                const operation: shard_state_store.DataOperation = switch (transition.kind) {
                    .finalize => .{ .finalize_split = .{
                        .new_shard_id = transition.destination_group_id,
                        .split_key = split_key,
                    } },
                    .rollback => .{ .rollback_split = .{
                        .new_shard_id = transition.destination_group_id,
                        .split_key = split_key,
                    } },
                    else => unreachable,
                };
                try operations.append(alloc, operation);
            },
        };
        if (decoded.batch.req.split_checkpoint) |checkpoint| {
            if (checkpoint.kind == .source_ack) {
                try operations.append(alloc, .{ .acknowledge_split = .{
                    .destination_group_id = checkpoint.destination_group_id,
                    .delta_sequence = checkpoint.delta_sequence,
                } });
            }
        }
    }
};

test "data raft apply store persists batches across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const encoded = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 3, .index = 14, .entry_type = .normal, .data = @constCast("put:a=") },
            .{ .term = 3, .index = 15, .entry_type = .normal, .data = @constCast("put:b=") },
        });
        defer std.testing.allocator.free(encoded);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 31,
            .commit_index = 15,
            .entries_bytes = encoded,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const batch = (try store.latestBatch(31)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 15), batch.commit_index);
        try std.testing.expectEqual(@as(usize, 2), batch.entry_count);
        try std.testing.expectEqual(@as(usize, 2), batch.normal_entry_count);
        try std.testing.expectEqual(@as(usize, 0), batch.admin_entry_count);
        try std.testing.expectEqual(@as(u64, 3), batch.last_entry_term);
        try std.testing.expectEqual(@as(u64, 15), batch.last_entry_index);
        const normal_entries = try store.appliedNormalEntries(std.testing.allocator, 31);
        defer {
            for (normal_entries) |entry| std.testing.allocator.free(entry.data);
            std.testing.allocator.free(normal_entries);
        }
        try std.testing.expectEqual(@as(usize, 2), normal_entries.len);
        try std.testing.expectEqual(@as(u64, 14), normal_entries[0].index);
        try std.testing.expectEqualStrings("put:b=", normal_entries[1].data);
        const group_state = try store.groupState(std.testing.allocator, 31);
        defer {
            for (group_state) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
            std.testing.allocator.free(group_state);
        }
        try std.testing.expectEqual(@as(usize, 2), group_state.len);
        try std.testing.expectEqualStrings("a", group_state[0].key);
        try std.testing.expectEqualStrings("", group_state[0].value);
        try std.testing.expectEqualStrings("b", group_state[1].key);
        try std.testing.expectEqualStrings("", group_state[1].value);
    }
}

test "data raft apply store admits one writable owner per root" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-single-writer", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var owner = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer owner.deinit();
    try std.testing.expectError(
        error.LsmRootWriterAlreadyOpen,
        RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root }),
    );
}

test "data raft apply store separates normal and admin entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-mixed", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const encoded = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 8, .index = 40, .entry_type = .normal, .data = @constCast("put:x=1") },
        .{ .term = 8, .index = 41, .entry_type = .conf_change, .data = @constCast("admin-1") },
        .{ .term = 8, .index = 42, .entry_type = .conf_change_v2, .data = @constCast("admin-2") },
        .{ .term = 8, .index = 43, .entry_type = .normal, .data = @constCast("put:y=2") },
    });
    defer std.testing.allocator.free(encoded);

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 44,
        .commit_index = 43,
        .entries_bytes = encoded,
    });

    const batch = (try store.latestBatch(44)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(usize, 4), batch.entry_count);
    try std.testing.expectEqual(@as(usize, 2), batch.normal_entry_count);
    try std.testing.expectEqual(@as(usize, 2), batch.admin_entry_count);
    const normal_entries = try store.appliedNormalEntries(std.testing.allocator, 44);
    defer {
        for (normal_entries) |entry| std.testing.allocator.free(entry.data);
        std.testing.allocator.free(normal_entries);
    }
    try std.testing.expectEqual(@as(usize, 2), normal_entries.len);
    try std.testing.expectEqual(@as(u64, 40), normal_entries[0].index);
    try std.testing.expectEqual(@as(u64, 43), normal_entries[1].index);
    try std.testing.expectEqualStrings("put:y=2", normal_entries[1].data);

    const group_state = try store.groupState(std.testing.allocator, 44);
    defer {
        for (group_state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(group_state);
    }
    try std.testing.expectEqual(@as(usize, 2), group_state.len);
    try std.testing.expectEqualStrings("x", group_state[0].key);
    try std.testing.expectEqualStrings("1", group_state[0].value);
    try std.testing.expectEqualStrings("y", group_state[1].key);
    try std.testing.expectEqualStrings("2", group_state[1].value);

    const snapshot = try store.snapshotBuilder().buildSnapshot(std.testing.allocator, 44);
    defer std.testing.allocator.free(snapshot);
    try std.testing.expect(snapshot.len > 4);
}

test "data raft apply store applies delete operations into group state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-delete", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const first = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("put:k=1") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:z=9") },
    });
    defer std.testing.allocator.free(first);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 2,
        .entries_bytes = first,
    });
    const first_snapshot = (try store.latestBatch(77)) orelse return error.MissingDataBatch;

    const second = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("del:k") },
    });
    defer std.testing.allocator.free(second);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 3,
        .entries_bytes = second,
    });
    try std.testing.expectEqual(@as(u64, 2), first_snapshot.commit_index);
    const second_snapshot = (try store.latestBatch(77)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 3), second_snapshot.commit_index);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 3,
        .entries_bytes = second,
    });
    const conflicting = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("put:z=10") },
    });
    defer std.testing.allocator.free(conflicting);
    try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 3,
        .entries_bytes = conflicting,
    }));
    try std.testing.expectError(error.OutOfOrderDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 2,
        .entries_bytes = first,
    }));

    const group_state = try store.groupState(std.testing.allocator, 77);
    defer {
        for (group_state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(group_state);
    }
    try std.testing.expectEqual(@as(usize, 1), group_state.len);
    try std.testing.expectEqualStrings("z", group_state[0].key);
    try std.testing.expectEqualStrings("9", group_state[0].value);
}

test "data raft apply store orders independent groups through separate shards" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-concurrent-groups", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.heap.page_allocator, .{ .root_dir = root });
    defer store.deinit();
    try std.testing.expect(store.batchShard(1) != store.batchShard(2));
    try std.testing.expect(store.batchShard(1) == store.batchShard(1 + batch_shard_count));

    const Worker = struct {
        store: *RaftApplyStore,
        group_id: u64,
        result: ?anyerror = null,

        fn run(self: *@This()) void {
            const entry = raft_state_machine.encodeCommittedEntries(std.heap.page_allocator, &.{.{
                .term = 1,
                .index = 1,
                .entry_type = .normal,
                .data = @constCast("put:k=1"),
            }}) catch |err| {
                self.result = err;
                return;
            };
            defer std.heap.page_allocator.free(entry);
            self.store.snapshotBuilder().applyBatch(.{
                .group_id = self.group_id,
                .commit_index = 1,
                .entries_bytes = entry,
            }) catch |err| {
                self.result = err;
            };
        }
    };

    var first = Worker{ .store = &store, .group_id = 1 };
    var second = Worker{ .store = &store, .group_id = 2 };
    const first_thread = try std.Thread.spawn(.{}, Worker.run, .{&first});
    const second_thread = try std.Thread.spawn(.{}, Worker.run, .{&second});
    first_thread.join();
    second_thread.join();
    if (first.result) |err| return err;
    if (second.result) |err| return err;
    try std.testing.expectEqual(@as(u64, 1), (try store.latestBatch(1)).?.commit_index);
    try std.testing.expectEqual(@as(u64, 1), (try store.latestBatch(2)).?.commit_index);
}

test "data raft apply store persists and enforces group range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-range", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const set_range = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:b:d") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(set_range);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 88,
            .commit_index = 2,
            .entries_bytes = set_range,
        });

        const byte_range = try store.currentRange(std.testing.allocator, 88);
        defer {
            if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
            if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
        }
        try std.testing.expectEqualStrings("b", byte_range.start);
        try std.testing.expectEqualStrings("d", byte_range.end);

        const out_of_range = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:a=1") },
        });
        defer std.testing.allocator.free(out_of_range);
        try std.testing.expectError(error.KeyOutOfRange, store.snapshotBuilder().applyBatch(.{
            .group_id = 88,
            .commit_index = 3,
            .entries_bytes = out_of_range,
        }));
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const byte_range = try store.currentRange(std.testing.allocator, 88);
        defer {
            if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
            if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
        }
        try std.testing.expectEqualStrings("b", byte_range.start);
        try std.testing.expectEqualStrings("d", byte_range.end);

        const group_state = try store.groupState(std.testing.allocator, 88);
        defer {
            for (group_state) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
            std.testing.allocator.free(group_state);
        }
        try std.testing.expectEqual(@as(usize, 1), group_state.len);
        try std.testing.expectEqualStrings("c", group_state[0].key);
        try std.testing.expectEqualStrings("3", group_state[0].value);
    }
}

test "data raft apply store parses empty-start colon range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-empty-start-range", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const set_range = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range::doc:024") },
    });
    defer std.testing.allocator.free(set_range);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 89,
        .commit_index = 1,
        .entries_bytes = set_range,
    });

    const byte_range = try store.currentRange(std.testing.allocator, 89);
    defer {
        if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
        if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
    }
    try std.testing.expectEqualStrings("", byte_range.start);
    try std.testing.expectEqualStrings("doc:024", byte_range.end);
}

test "data raft apply store captures split handoff and replays destination deltas" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var src = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer src.deinit();
    var dst = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dst.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b=left-0") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t=right-0") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:90:doc:m") },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("split_start:90:doc:m") },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = @constCast("put:doc:u=right-1") },
    });
    defer std.testing.allocator.free(setup);
    try src.snapshotBuilder().applyBatch(.{
        .group_id = 91,
        .commit_index = 6,
        .entries_bytes = setup,
    });

    const handoff = try src.captureSplitHandoff(std.testing.allocator, 91);
    defer shard_state_store.freeHandoff(std.testing.allocator, handoff);
    try std.testing.expectEqual(@as(u64, 1), handoff.base_delta_sequence);
    try dst.applySplitHandoff(std.testing.allocator, 92, handoff);

    const catchup_batch = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = @constCast("put:doc:c=left-1") },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = @constCast("put:doc:x=right-2") },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = @constCast("del:doc:t") },
    });
    defer std.testing.allocator.free(catchup_batch);
    try src.snapshotBuilder().applyBatch(.{
        .group_id = 91,
        .commit_index = 9,
        .entries_bytes = catchup_batch,
    });

    const deltas = try src.listSplitDeltasAfter(std.testing.allocator, 91, handoff.base_delta_sequence);
    defer shard_mod.freeDeltas(std.testing.allocator, deltas);
    try std.testing.expectEqual(@as(usize, 1), deltas.len);
    try dst.applySplitDeltas(std.testing.allocator, 92, deltas);

    const byte_range = try dst.currentRange(std.testing.allocator, 92);
    defer {
        if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
        if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
    }
    try std.testing.expectEqualStrings("doc:m", byte_range.start);
    try std.testing.expectEqualStrings("doc:z", byte_range.end);

    const state = try dst.groupState(std.testing.allocator, 92);
    defer shard_state_store.freeGroupStateEntries(std.testing.allocator, state);
    try std.testing.expectEqual(@as(usize, 2), state.len);
    try std.testing.expectEqualStrings("doc:u", state[0].key);
    try std.testing.expectEqualStrings("right-1", state[0].value);
    try std.testing.expectEqualStrings("doc:x", state[1].key);
    try std.testing.expectEqualStrings("right-2", state[1].value);
}

test "data raft apply store parses colon-delimited range keys correctly" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-range-colons", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const encoded = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:m={\"v\":1}") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_prepare:192:doc:n") },
    });
    defer std.testing.allocator.free(encoded);

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 191,
        .commit_index = 3,
        .entries_bytes = encoded,
    });

    const byte_range = try store.currentRange(std.testing.allocator, 191);
    defer {
        if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
        if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
    }
    try std.testing.expectEqualStrings("doc:a", byte_range.start);
    try std.testing.expectEqualStrings("doc:z", byte_range.end);

    const split_state = (try store.currentSplitState(std.testing.allocator, 191)) orelse return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, split_state);
    try std.testing.expectEqualStrings("doc:n", split_state.split_key);
}

test "data raft apply store persists split destination acknowledgements" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-ack", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const batch = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .source_ack,
            .source_group_id = 201,
            .destination_group_id = 202,
            .delta_sequence = 9,
        },
    });
    defer std.testing.allocator.free(batch);
    const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = batch },
    });
    defer std.testing.allocator.free(entries);

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 201,
        .commit_index = 1,
        .entries_bytes = entries,
    });
    const acknowledgement = (try store.currentSplitAcknowledgement(std.testing.allocator, 201)) orelse
        return error.MissingSplitAcknowledgement;
    try std.testing.expectEqual(@as(u64, 202), acknowledgement.destination_group_id);
    try std.testing.expectEqual(@as(u64, 9), acknowledgement.delta_sequence);
}

test "data raft apply store skips persisted split commands in overlapping replay" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-overlap", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .prepare,
            .destination_group_id = 212,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(prepare);
    const start = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .start,
            .destination_group_id = 212,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(start);

    const initial = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = prepare },
    });
    defer std.testing.allocator.free(initial);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 211,
        .commit_index = 2,
        .entries_bytes = initial,
    });

    const overlapping = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = prepare },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = start },
    });
    defer std.testing.allocator.free(overlapping);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 211,
        .commit_index = 3,
        .entries_bytes = overlapping,
    });

    const split_state = (try store.currentSplitState(std.testing.allocator, 211)) orelse
        return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, split_state);
    try std.testing.expectEqual(shard_mod.SplitPhase.splitting, split_state.phase);
    try std.testing.expectEqual(@as(u64, 212), split_state.new_shard_id);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);
}

test "data raft apply store recovers exact split replay after injected projection corruption" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-watermark-lag", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .prepare,
            .destination_group_id = 222,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(prepare);
    const range_entry = raft_engine.core.Entry{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = @constCast("range:doc:a:doc:z"),
    };
    const prepare_entry = raft_engine.core.Entry{
        .term = 1,
        .index = 2,
        .entry_type = .normal,
        .data = prepare,
    };

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const applied = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{ range_entry, prepare_entry });
        defer std.testing.allocator.free(applied);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 221,
            .commit_index = 2,
            .entries_bytes = applied,
        });

        // Fault-inject a marker regression that the atomic putBatch path cannot
        // produce. Split operations remain exactly idempotent as a final line
        // of defense if storage is externally corrupted or restored unevenly.
        const lagging = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{range_entry});
        defer std.testing.allocator.free(lagging);
        var key_buf: [128]u8 = undefined;
        const key = try RaftApplyStore.keyForGroup(&key_buf, 221);
        const value = try std.testing.allocator.alloc(u8, @sizeOf(u64) + lagging.len);
        defer std.testing.allocator.free(value);
        std.mem.writeInt(u64, value[0..8], 1, .little);
        @memcpy(value[8..], lagging);
        try store.store.put(key, value);
    }

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    const replay = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{prepare_entry});
    defer std.testing.allocator.free(replay);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 2,
        .entries_bytes = replay,
    });

    const batch = (try store.latestBatch(221)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 2), batch.commit_index);
    try std.testing.expectEqual(@as(u64, 2), batch.last_entry_index);
    const split_state = (try store.currentSplitState(std.testing.allocator, 221)) orelse
        return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, split_state);
    try std.testing.expectEqual(shard_mod.SplitPhase.prepare, split_state.phase);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);
}

test "data raft apply store rejects mismatched terminal split identity" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-terminal-identity", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{ .kind = .prepare, .destination_group_id = 222, .split_key = "doc:m" },
    });
    defer std.testing.allocator.free(prepare);
    const start = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{ .kind = .start, .destination_group_id = 222, .split_key = "doc:m" },
    });
    defer std.testing.allocator.free(start);
    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = prepare },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = start },
    });
    defer std.testing.allocator.free(setup);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 3,
        .entries_bytes = setup,
    });

    const wrong_rollback = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{ .kind = .rollback, .destination_group_id = 223, .split_key = "doc:m" },
    });
    defer std.testing.allocator.free(wrong_rollback);
    const wrong_rollback_entry = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = wrong_rollback },
    });
    defer std.testing.allocator.free(wrong_rollback_entry);
    try std.testing.expectError(error.ConflictingSplitTransition, store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 4,
        .entries_bytes = wrong_rollback_entry,
    }));

    const wrong_ack = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .source_ack,
            .source_group_id = 221,
            .destination_group_id = 223,
            .delta_sequence = 0,
        },
    });
    defer std.testing.allocator.free(wrong_ack);
    const wrong_ack_entry = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = wrong_ack },
    });
    defer std.testing.allocator.free(wrong_ack_entry);
    try std.testing.expectError(error.ConflictingSplitTransition, store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 4,
        .entries_bytes = wrong_ack_entry,
    }));

    const state = (try store.currentSplitState(std.testing.allocator, 221)) orelse return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, state);
    try std.testing.expectEqual(shard_mod.SplitPhase.splitting, state.phase);
    try std.testing.expectEqual(@as(u64, 222), state.new_shard_id);
}

test "data raft apply store seeds pre-raft snapshots once at reserved index zero" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-seed", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        try std.testing.expect(try store.seedGroupSnapshotIfAbsent(
            std.testing.allocator,
            311,
            .{ .start = "doc:a", .end = "doc:z" },
            &.{.{ .key = "doc:a", .value = "{\"v\":1}" }},
        ));
        const baseline = (try store.latestBatch(311)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 0), baseline.commit_index);
        try std.testing.expectEqual(@as(u64, 0), baseline.last_entry_index);

        try std.testing.expect(!try store.seedGroupSnapshotIfAbsent(
            std.testing.allocator,
            311,
            .{ .start = "doc:m", .end = "" },
            &.{.{ .key = "doc:replacement", .value = "{}" }},
        ));
    }

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    const restored_baseline = (try store.latestBatch(311)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 0), restored_baseline.commit_index);
    try std.testing.expectEqual(@as(u64, 0), restored_baseline.last_entry_index);

    const real = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = @constCast("put:doc:b={\"v\":2}"),
    }});
    defer std.testing.allocator.free(real);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 311,
        .commit_index = 1,
        .entries_bytes = real,
    });

    const state = try store.groupState(std.testing.allocator, 311);
    defer shard_state_store.freeGroupStateEntries(std.testing.allocator, state);
    try std.testing.expectEqual(@as(usize, 2), state.len);
    try std.testing.expectEqualStrings("doc:a", state[0].key);
    try std.testing.expectEqualStrings("doc:b", state[1].key);
}

test "data apply store replay is idempotent when applied watermark lags WAL state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-replay-idempotent", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try raft_storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 201, 1);
    defer layout.deinit(std.testing.allocator);

    const first_data = try std.testing.allocator.dupe(u8, "put:doc:a=1");
    defer std.testing.allocator.free(first_data);
    const second_data = try std.testing.allocator.dupe(u8, "put:doc:b=2");
    defer std.testing.allocator.free(second_data);

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();

        const entries = try std.testing.allocator.dupe(raft_engine.core.Entry, &[_]raft_engine.core.Entry{
            .{ .term = 4, .index = 1, .entry_type = .normal, .data = first_data },
            .{ .term = 4, .index = 2, .entry_type = .normal, .data = second_data },
        });
        defer std.testing.allocator.free(entries);

        try wal_state.groupStorage().persistReady(201, .{
            .hard_state = .{ .current_term = 4, .voted_for = 1, .commit_index = 2 },
            .entries = entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.DataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };

        try sm.stateMachine().applyReady(201, &.{
            .{ .term = 4, .index = 1, .entry_type = .normal, .data = @constCast("put:doc:a=1") },
            .{ .term = 4, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b=2") },
        }, &.{});
    }

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();
        try std.testing.expectEqual(@as(u64, 0), wal_state.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 201,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = wal_state.appliedIndex(),
        }, wal_state.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);

        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.DataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(201, rd.committed_entries, &.{});

        const batch = (try store.latestBatch(201)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 2), batch.commit_index);
        try std.testing.expectEqual(@as(usize, 2), batch.entry_count);

        const state = try store.groupState(std.testing.allocator, 201);
        defer shard_state_store.freeGroupStateEntries(std.testing.allocator, state);
        try std.testing.expectEqual(@as(usize, 2), state.len);
        try std.testing.expectEqualStrings("doc:a", state[0].key);
        try std.testing.expectEqualStrings("1", state[0].value);
        try std.testing.expectEqualStrings("doc:b", state[1].key);
        try std.testing.expectEqualStrings("2", state[1].value);

        const normal_entries = try store.appliedNormalEntries(std.testing.allocator, 201);
        defer {
            for (normal_entries) |entry| std.testing.allocator.free(entry.data);
            std.testing.allocator.free(normal_entries);
        }
        try std.testing.expectEqual(@as(usize, 2), normal_entries.len);
        try std.testing.expectEqual(@as(u64, 1), normal_entries[0].index);
        try std.testing.expectEqual(@as(u64, 2), normal_entries[1].index);
    }
}
