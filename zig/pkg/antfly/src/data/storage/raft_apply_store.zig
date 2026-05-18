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

pub const AppliedDataBatch = struct {
    commit_index: u64,
    entry_count: usize,
    normal_entry_count: usize,
    admin_entry_count: usize,
    last_entry_term: u64,
    last_entry_index: u64,
    last_normal_data: ?[]const u8,
    entries_bytes: []const u8,
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
};

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    path: []u8,
    backend: lsm_backend.BackendHandle,
    store: docstore.DocStore,
    batches: std.AutoHashMapUnmanaged(u64, OwnedBatch) = .empty,

    const OwnedBatch = struct {
        commit_index: u64,
        entry_count: usize,
        normal_entry_count: usize,
        admin_entry_count: usize,
        last_entry_term: u64,
        last_entry_index: u64,
        last_normal_data: ?[]u8,
        entries_bytes: []u8,
    };

    pub fn init(alloc: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();

        const root_dir = try alloc.dupe(u8, cfg.root_dir);
        errdefer alloc.free(root_dir);

        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);

        const path = try std.fmt.allocPrint(alloc, "{s}/data-apply-store", .{root_dir});
        errdefer alloc.free(path);
        try fs_paths.createDirPathPortable(io_impl.io(), path);

        var backend = try lsm_backend.BackendHandle.open(alloc, path, .{
            .backend = .{
                .durability = if (cfg.no_sync) .none else .full,
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
        var it = self.batches.valueIterator();
        while (it.next()) |batch| {
            self.alloc.free(batch.entries_bytes);
            if (batch.last_normal_data) |data| self.alloc.free(data);
        }
        self.batches.deinit(self.alloc);
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
        const batch = (try self.ensureLoaded(group_id)) orelse return null;
        return .{
            .commit_index = batch.commit_index,
            .entry_count = batch.entry_count,
            .normal_entry_count = batch.normal_entry_count,
            .admin_entry_count = batch.admin_entry_count,
            .last_entry_term = batch.last_entry_term,
            .last_entry_index = batch.last_entry_index,
            .last_normal_data = batch.last_normal_data,
            .entries_bytes = batch.entries_bytes,
        };
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

    pub fn currentRange(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !AppliedDataRange {
        return try shard_state_store.currentRange(&self.store, alloc, group_id);
    }

    pub fn currentSplitState(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?AppliedSplitState {
        return try shard_state_store.currentSplitState(&self.store, alloc, group_id);
    }

    pub fn currentSplitDeltaSequence(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !u64 {
        return try shard_state_store.currentSplitDeltaSequence(&self.store, alloc, group_id);
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
        const metadata = try describeEntries(self.alloc, entries_bytes);
        defer if (metadata.last_normal_data) |data| self.alloc.free(data);
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
                .prepare_split => |split_key| self.alloc.free(split_key),
                .start_split => |start| self.alloc.free(start.split_key),
                .finalize_split, .rollback_split => {},
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
        var value = try self.alloc.alloc(u8, @sizeOf(u64) + entries_bytes.len);
        std.mem.writeInt(u64, value[0..8], commit_index, .little);
        @memcpy(value[8..], entries_bytes);
        try writes.append(self.alloc, .{
            .key = try self.alloc.dupe(u8, key),
            .value = value,
        });

        for (metadata.normal_entries) |entry| {
            var normal_key_buf: [160]u8 = undefined;
            const normal_key = try normalEntryKeyForGroup(&normal_key_buf, group_id, entry.index);
            try writes.append(self.alloc, .{
                .key = try self.alloc.dupe(u8, normal_key),
                .value = try self.alloc.dupe(u8, entry.data),
            });
        }
        try shard_state_store.appendOperationEffects(&self.store, self.alloc, group_id, metadata.operations, &writes, &deletes);
        try shard_state_store.putOwnedBatch(&self.store, self.alloc, writes.items, deletes.items);

        const owned_entries = try self.alloc.dupe(u8, entries_bytes);
        errdefer self.alloc.free(owned_entries);
        const owned_last_normal_data = if (metadata.last_normal_data) |data|
            try self.alloc.dupe(u8, data)
        else
            null;
        errdefer if (owned_last_normal_data) |data| self.alloc.free(data);
        if (self.batches.getPtr(group_id)) |existing| {
            self.alloc.free(existing.entries_bytes);
            if (existing.last_normal_data) |data| self.alloc.free(data);
            existing.* = .{
                .commit_index = commit_index,
                .entry_count = metadata.entry_count,
                .normal_entry_count = metadata.normal_entry_count,
                .admin_entry_count = metadata.admin_entry_count,
                .last_entry_term = metadata.last_entry_term,
                .last_entry_index = metadata.last_entry_index,
                .last_normal_data = owned_last_normal_data,
                .entries_bytes = owned_entries,
            };
            return;
        }
        try self.batches.put(self.alloc, group_id, .{
            .commit_index = commit_index,
            .entry_count = metadata.entry_count,
            .normal_entry_count = metadata.normal_entry_count,
            .admin_entry_count = metadata.admin_entry_count,
            .last_entry_term = metadata.last_entry_term,
            .last_entry_index = metadata.last_entry_index,
            .last_normal_data = owned_last_normal_data,
            .entries_bytes = owned_entries,
        });
    }

    fn ensureLoaded(self: *RaftApplyStore, group_id: u64) !?*OwnedBatch {
        if (self.batches.getPtr(group_id)) |batch| return batch;

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidDataApplyBatch;

        const commit_index = std.mem.readInt(u64, encoded[0..8], .little);
        const metadata = try describeEntries(self.alloc, encoded[8..]);
        defer if (metadata.last_normal_data) |data| self.alloc.free(data);
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
                .prepare_split => |split_key| self.alloc.free(split_key),
                .start_split => |start| self.alloc.free(start.split_key),
                .finalize_split, .rollback_split => {},
            };
            self.alloc.free(metadata.operations);
        }
        const owned_entries = try self.alloc.dupe(u8, encoded[8..]);
        errdefer self.alloc.free(owned_entries);
        const owned_last_normal_data = if (metadata.last_normal_data) |data|
            try self.alloc.dupe(u8, data)
        else
            null;
        errdefer if (owned_last_normal_data) |data| self.alloc.free(data);
        try self.batches.put(self.alloc, group_id, .{
            .commit_index = commit_index,
            .entry_count = metadata.entry_count,
            .normal_entry_count = metadata.normal_entry_count,
            .admin_entry_count = metadata.admin_entry_count,
            .last_entry_term = metadata.last_entry_term,
            .last_entry_index = metadata.last_entry_index,
            .last_normal_data = owned_last_normal_data,
            .entries_bytes = owned_entries,
        });
        return self.batches.getPtr(group_id);
    }

    const EntryMetadata = struct {
        entry_count: usize,
        normal_entry_count: usize,
        admin_entry_count: usize,
        last_entry_term: u64,
        last_entry_index: u64,
        last_normal_data: ?[]u8,
        normal_entries: []AppliedNormalEntry,
        operations: []DataOperation,
    };

    const DataOperation = shard_state_store.DataOperation;

    fn describeEntries(alloc: std.mem.Allocator, entries_bytes: []const u8) !EntryMetadata {
        const decoded = try raft_state_machine.decodeCommittedEntries(alloc, entries_bytes);
        defer alloc.free(decoded);
        var normal_entry_count: usize = 0;
        var admin_entry_count: usize = 0;
        var last_normal_data: ?[]u8 = null;
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
                .prepare_split => |split_key| alloc.free(split_key),
                .start_split => |start| alloc.free(start.split_key),
                .finalize_split, .rollback_split => {},
            };
            operations.deinit(alloc);
        }
        errdefer if (last_normal_data) |data| alloc.free(data);
        for (decoded) |entry| {
            switch (entry.entry_type) {
                .normal => {
                    normal_entry_count += 1;
                    try normal_entries.append(alloc, .{
                        .index = entry.index,
                        .data = try alloc.dupe(u8, entry.data),
                    });
                    if (last_normal_data) |existing| alloc.free(existing);
                    last_normal_data = try alloc.dupe(u8, entry.data);
                    if (try parseDataOperation(alloc, entry.data)) |op| {
                        try operations.append(alloc, op);
                    }
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
                .last_normal_data = null,
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
            .last_normal_data = last_normal_data,
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
            return .{ .prepare_split = try alloc.dupe(u8, data["split_prepare:".len..]) };
        }
        if (std.mem.startsWith(u8, data, "split_start:")) {
            const payload = data["split_start:".len..];
            if (std.mem.indexOfScalar(u8, payload, ':')) |sep| {
                return .{ .start_split = .{
                    .new_shard_id = try std.fmt.parseInt(u64, payload[0..sep], 10),
                    .split_key = try alloc.dupe(u8, payload[sep + 1 ..]),
                } };
            }
            return error.InvalidAppliedDataRange;
        }
        if (std.mem.eql(u8, data, "split_finalize") or std.mem.eql(u8, data, "finalize_split")) {
            return .finalize_split;
        }
        if (std.mem.eql(u8, data, "split_rollback") or std.mem.eql(u8, data, "rollback_split")) {
            return .rollback_split;
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
        try std.testing.expectEqualStrings("put:b=", batch.last_normal_data orelse return error.MissingLastNormalData);
        const decoded = try raft_state_machine.decodeCommittedEntries(std.testing.allocator, batch.entries_bytes);
        defer std.testing.allocator.free(decoded);
        try std.testing.expectEqualStrings("put:b=", decoded[1].data);
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
    try std.testing.expectEqualStrings("put:y=2", batch.last_normal_data orelse return error.MissingLastNormalData);

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

    const second = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("del:k") },
    });
    defer std.testing.allocator.free(second);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 3,
        .entries_bytes = second,
    });

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
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
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
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_prepare:doc:n") },
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
