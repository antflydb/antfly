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
const fs_paths = @import("../../common/fs_paths.zig");
const raft_engine = @import("raft_engine");
const platform_time = @import("../../platform/time.zig");
const wal_mod = @import("../../storage/wal.zig");
const storage_mod = @import("mod.zig");

const magic: u32 = 0x41524654; // ARFT
const version: u32 = 2;
const delta_magic: u32 = 0x4152444c; // ARDL
const delta_version: u32 = 1;
const applied_watermark_magic: u32 = 0x4152574d; // ARWM
const applied_watermark_version: u32 = 1;

var applied_watermark_tmp_nonce: std.atomic.Value(u64) = .init(0);

pub const WalReplicaStateConfig = struct {
    wal: wal_mod.WalOptions = .{ .backend = .lsm },
    checkpoint_replay_records_threshold: usize = 64,
    checkpoint_replay_bytes_threshold: usize = 256 * 1024,
    applied_watermark_persist_interval: u64 = 64,
    compaction_retained_entries: u64 = 4096,
    compaction_min_interval_entries: u64 = 4096,
    compaction_single_node_only: bool = true,
};

pub const WalReplicaStateStats = struct {
    persist_ready_calls: u64 = 0,
    applied_index_updates: u64 = 0,
    conf_state_updates: u64 = 0,
    ready_persist_calls: u64 = 0,
    applied_index_persist_calls: u64 = 0,
    conf_state_persist_calls: u64 = 0,
    checkpoint_persist_calls: u64 = 0,
    persist_ns: u64 = 0,
    encode_ns: u64 = 0,
    wal_append_ns: u64 = 0,
    wal_truncate_ns: u64 = 0,
    encoded_bytes: u64 = 0,
    applied_watermark_persist_ns: u64 = 0,
    applied_watermark_bytes: u64 = 0,
    replay_debt_records: u64 = 0,
    replay_debt_bytes: u64 = 0,
    replayed_delta_records: u64 = 0,
    replayed_delta_bytes: u64 = 0,
    storage_compactions: u64 = 0,
    storage_compaction_ns: u64 = 0,
    max_storage_compaction_ns: u64 = 0,
    last_compacted_index: u64 = 0,
};

const PersistReason = enum {
    ready,
    applied_index,
    conf_state,
    checkpoint,
};

const DeltaRecordKind = enum(u8) {
    ready = 1,
    conf_state = 2,
};

pub const WalReplicaState = struct {
    alloc: std.mem.Allocator,
    cfg: WalReplicaStateConfig,
    io_impl: std.Io.Threaded,
    layout: storage_mod.ReplicaPathLayout,
    wal_dir: []u8,
    wal_dir_z: [:0]u8,
    applied_watermark_path: []u8,
    wal: wal_mod.WAL,
    store: raft_engine.core.MemoryStorage,
    applied_index: raft_engine.core.types.Index = 0,
    durable_applied_index: raft_engine.core.types.Index = 0,
    last_compacted_index: raft_engine.core.types.Index = 0,
    delta_records_since_checkpoint: usize = 0,
    delta_bytes_since_checkpoint: usize = 0,
    stats: WalReplicaStateStats = .{},

    pub fn init(
        alloc: std.mem.Allocator,
        layout: storage_mod.ReplicaPathLayout,
        cfg: WalReplicaStateConfig,
    ) !WalReplicaState {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();

        try fs_paths.createDirPathPortable(io_impl.io(), layout.log_dir);
        const wal_dir = try std.fmt.allocPrint(alloc, "{s}/state-wal", .{layout.log_dir});
        errdefer alloc.free(wal_dir);
        try fs_paths.createDirPathPortable(io_impl.io(), wal_dir);
        const wal_dir_z = try alloc.dupeSentinel(u8, wal_dir, 0);
        errdefer alloc.free(wal_dir_z);
        const applied_watermark_path = try std.fmt.allocPrint(alloc, "{s}/applied-watermark.bin", .{layout.log_dir});
        errdefer alloc.free(applied_watermark_path);

        var self = WalReplicaState{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = io_impl,
            .layout = .{
                .root_dir = try alloc.dupe(u8, layout.root_dir),
                .log_dir = try alloc.dupe(u8, layout.log_dir),
                .snapshot_dir = try alloc.dupe(u8, layout.snapshot_dir),
            },
            .wal_dir = wal_dir,
            .wal_dir_z = wal_dir_z,
            .applied_watermark_path = applied_watermark_path,
            .wal = try wal_mod.WAL.open(wal_dir_z.ptr, cfg.wal),
            .store = raft_engine.core.MemoryStorage.init(alloc),
        };
        errdefer self.deinit();
        try self.load();
        return self;
    }

    pub fn deinit(self: *WalReplicaState) void {
        self.store.deinit();
        self.wal.close();
        self.alloc.free(self.wal_dir_z);
        self.alloc.free(self.wal_dir);
        self.alloc.free(self.applied_watermark_path);
        self.layout.deinit(self.alloc);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn storage(self: *WalReplicaState) raft_engine.core.Storage {
        return self.store.storage();
    }

    pub fn groupStorage(self: *WalReplicaState) raft_engine.runtime.storage_iface.GroupStorage {
        return .{
            .ptr = self,
            .vtable = &.{
                .persist_ready = persistReady,
            },
        };
    }

    pub fn setConfState(self: *WalReplicaState, conf_state: raft_engine.core.ConfState) !void {
        try self.store.setConfState(conf_state);
        self.stats.conf_state_updates += 1;
        try self.persistConfStateDelta();
    }

    pub fn seedConfStateIfEmpty(self: *WalReplicaState, peers: []const raft_engine.core.types.NodeId) !void {
        if (peers.len == 0) return;
        var initial_state = try self.store.storage().initialState(self.alloc);
        defer initial_state.deinit(self.alloc);
        if (initial_state.conf_state.voters.len > 0 or
            initial_state.conf_state.voters_outgoing.len > 0 or
            initial_state.conf_state.learners.len > 0 or
            initial_state.conf_state.learners_next.len > 0 or
            initial_state.conf_state.auto_leave)
        {
            return;
        }
        const conf_state = raft_engine.core.ConfState{
            .voters = @constCast(peers),
        };
        try self.setConfState(conf_state);
    }

    pub fn appliedIndex(self: *const WalReplicaState) raft_engine.core.types.Index {
        return self.applied_index;
    }

    pub fn statsSnapshot(self: *const WalReplicaState) WalReplicaStateStats {
        var stats = self.stats;
        stats.replay_debt_records = self.delta_records_since_checkpoint;
        stats.replay_debt_bytes = self.delta_bytes_since_checkpoint;
        stats.last_compacted_index = self.last_compacted_index;
        return stats;
    }

    pub fn flushForShutdown(self: *WalReplicaState) !void {
        if (self.delta_records_since_checkpoint > 0 or self.delta_bytes_since_checkpoint > 0) {
            try self.persistCheckpoint();
            self.delta_records_since_checkpoint = 0;
            self.delta_bytes_since_checkpoint = 0;
        }
        try self.persistAppliedWatermark();
        try self.wal.sync(true);
    }

    pub fn setAppliedIndex(self: *WalReplicaState, index: raft_engine.core.types.Index) !void {
        if (index <= self.applied_index) return;
        self.applied_index = index;
        self.stats.applied_index_updates += 1;
        if (self.shouldPersistAppliedWatermark(index)) try self.persistAppliedWatermark();
        try self.compactAppliedStorageIfNeeded();
        try self.persistCheckpointIfNeeded();
    }

    fn persistReady(ptr: *anyopaque, group_id: u64, ready: raft_engine.core.Ready) !void {
        _ = group_id;
        const self: *WalReplicaState = @ptrCast(@alignCast(ptr));
        self.stats.persist_ready_calls += 1;
        if (ready.snapshot) |snapshot| {
            try self.store.applySnapshot(snapshot);
            if (snapshot.metadata.index > self.applied_index) self.applied_index = snapshot.metadata.index;
        }
        if (ready.hard_state) |hard_state| self.store.setHardState(hard_state);
        if (ready.entries.len > 0) try self.store.append(ready.entries);
        try self.persistReadyDelta(ready);
    }

    fn load(self: *WalReplicaState) !void {
        const entries = try self.wal.iterateFrom(self.alloc, 1);
        defer {
            for (entries) |entry| self.alloc.free(@constCast(entry.data));
            self.alloc.free(entries);
        }
        if (entries.len == 0) return;

        var replay_from: usize = 0;
        for (entries, 0..) |entry, i| {
            const record_magic = peekRecordMagic(entry.data) catch continue;
            if (record_magic == magic) replay_from = i;
        }

        for (entries[replay_from..]) |entry| {
            try self.decodeWalRecord(entry.data);
        }
        try self.loadAppliedWatermark();
        self.durable_applied_index = self.applied_index;
        try self.refreshLastCompactedIndex();
    }

    fn persist(self: *WalReplicaState, reason: PersistReason) !void {
        const started_ns = nowNs();
        switch (reason) {
            .ready => self.stats.ready_persist_calls += 1,
            .applied_index => self.stats.applied_index_persist_calls += 1,
            .conf_state => self.stats.conf_state_persist_calls += 1,
            .checkpoint => self.stats.checkpoint_persist_calls += 1,
        }

        const encode_started_ns = nowNs();
        const encoded = try self.encodeCurrentState();
        self.stats.encode_ns += elapsedSince(encode_started_ns);
        defer self.alloc.free(encoded);
        self.stats.encoded_bytes += encoded.len;

        const append_started_ns = nowNs();
        const lsn = try self.wal.append(encoded);
        self.stats.wal_append_ns += elapsedSince(append_started_ns);
        if (reason == .checkpoint and lsn > 1) {
            const truncate_started_ns = nowNs();
            try self.wal.truncate(lsn - 1);
            self.stats.wal_truncate_ns += elapsedSince(truncate_started_ns);
        }
        self.stats.persist_ns += elapsedSince(started_ns);
    }

    fn encodeCurrentState(self: *WalReplicaState) ![]u8 {
        var buffer = std.ArrayListUnmanaged(u8).empty;
        errdefer buffer.deinit(self.alloc);

        try appendInt(u32, self.alloc, &buffer, magic);
        try appendInt(u32, self.alloc, &buffer, version);

        var initial_state = try self.store.storage().initialState(self.alloc);
        defer initial_state.deinit(self.alloc);
        try appendInt(u64, self.alloc, &buffer, initial_state.hard_state.current_term);
        try appendBool(self.alloc, &buffer, initial_state.hard_state.voted_for != null);
        if (initial_state.hard_state.voted_for) |voted_for| try appendInt(u64, self.alloc, &buffer, voted_for);
        try appendInt(u64, self.alloc, &buffer, initial_state.hard_state.commit_index);
        try appendInt(u64, self.alloc, &buffer, self.applied_index);
        try encodeConfState(self.alloc, &buffer, initial_state.conf_state);

        const snapshot = try self.store.storage().snapshot(self.alloc);
        defer {
            var owned = snapshot;
            owned.deinit(self.alloc);
        }
        const has_snapshot = snapshot.metadata.index != 0 or snapshot.metadata.term != 0 or snapshot.data.len > 0 or snapshot.metadata.conf_state.voters.len > 0;
        try appendBool(self.alloc, &buffer, has_snapshot);
        if (has_snapshot) try encodeSnapshot(self.alloc, &buffer, snapshot);

        const first_index = try self.store.storage().firstIndex();
        const last_index = try self.store.storage().lastIndex();
        const persisted_entries = if (last_index + 1 > first_index)
            try self.store.storage().entries(self.alloc, first_index, last_index + 1, 0)
        else
            try self.alloc.dupe(raft_engine.core.Entry, &.{});
        defer raft_engine.core.types.freeEntries(self.alloc, persisted_entries);

        try appendInt(u32, self.alloc, &buffer, @intCast(persisted_entries.len));
        for (persisted_entries) |entry| try encodeEntry(self.alloc, &buffer, entry);

        return try buffer.toOwnedSlice(self.alloc);
    }

    fn persistReadyDelta(self: *WalReplicaState, ready: raft_engine.core.Ready) !void {
        const started_ns = nowNs();
        self.stats.ready_persist_calls += 1;

        const encode_started_ns = nowNs();
        const encoded = try self.encodeReadyDelta(ready);
        self.stats.encode_ns += elapsedSince(encode_started_ns);
        defer self.alloc.free(encoded);
        self.stats.encoded_bytes += encoded.len;

        const append_started_ns = nowNs();
        _ = try self.wal.append(encoded);
        self.stats.wal_append_ns += elapsedSince(append_started_ns);
        self.stats.persist_ns += elapsedSince(started_ns);
        self.deltaRecordsPersisted(encoded.len);
    }

    fn persistConfStateDelta(self: *WalReplicaState) !void {
        const started_ns = nowNs();
        self.stats.conf_state_persist_calls += 1;

        const encode_started_ns = nowNs();
        const encoded = try self.encodeConfStateDelta();
        self.stats.encode_ns += elapsedSince(encode_started_ns);
        defer self.alloc.free(encoded);
        self.stats.encoded_bytes += encoded.len;

        const append_started_ns = nowNs();
        _ = try self.wal.append(encoded);
        self.stats.wal_append_ns += elapsedSince(append_started_ns);
        self.stats.persist_ns += elapsedSince(started_ns);
        self.deltaRecordsPersisted(encoded.len);
        try self.persistCheckpointIfNeeded();
    }

    fn deltaRecordsPersisted(self: *WalReplicaState, encoded_len: usize) void {
        self.delta_records_since_checkpoint +|= 1;
        self.delta_bytes_since_checkpoint +|= encoded_len;
    }

    fn persistCheckpointIfNeeded(self: *WalReplicaState) !void {
        const records_over_threshold = self.cfg.checkpoint_replay_records_threshold > 0 and
            self.delta_records_since_checkpoint >= self.cfg.checkpoint_replay_records_threshold;
        const bytes_over_threshold = self.cfg.checkpoint_replay_bytes_threshold > 0 and
            self.delta_bytes_since_checkpoint >= self.cfg.checkpoint_replay_bytes_threshold;
        if (!records_over_threshold and !bytes_over_threshold) return;
        try self.persistCheckpoint();
        self.delta_records_since_checkpoint = 0;
        self.delta_bytes_since_checkpoint = 0;
    }

    fn persistCheckpoint(self: *WalReplicaState) !void {
        try self.persist(.checkpoint);
        self.durable_applied_index = self.applied_index;
    }

    fn persistAppliedWatermark(self: *WalReplicaState) !void {
        self.stats.applied_index_persist_calls += 1;
        const started_ns = nowNs();
        var payload: [16]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], applied_watermark_magic, .little);
        std.mem.writeInt(u32, payload[4..8], applied_watermark_version, .little);
        std.mem.writeInt(u64, payload[8..16], self.applied_index, .little);
        try writeFileAtomically(self.io_impl.io(), self.applied_watermark_path, &payload);
        self.durable_applied_index = self.applied_index;
        self.stats.applied_watermark_persist_ns += elapsedSince(started_ns);
        self.stats.applied_watermark_bytes += payload.len;
    }

    fn shouldPersistAppliedWatermark(self: *const WalReplicaState, index: raft_engine.core.types.Index) bool {
        if (index <= self.durable_applied_index) return false;
        if (self.durable_applied_index == 0) return true;
        const interval = self.cfg.applied_watermark_persist_interval;
        if (interval == 0) return false;
        return index - self.durable_applied_index >= interval;
    }

    fn loadAppliedWatermark(self: *WalReplicaState) !void {
        var file = std.Io.Dir.cwd().openFile(self.io_impl.io(), self.applied_watermark_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer file.close(self.io_impl.io());

        var reader = file.reader(self.io_impl.io(), &.{});
        var payload: [16]u8 = undefined;
        try reader.interface.readSliceAll(&payload);

        const file_magic = std.mem.readInt(u32, payload[0..4], .little);
        if (file_magic != applied_watermark_magic) return error.InvalidReplicaState;
        const file_version = std.mem.readInt(u32, payload[4..8], .little);
        if (file_version != applied_watermark_version) return error.UnsupportedReplicaStateVersion;
        const watermark = std.mem.readInt(u64, payload[8..16], .little);
        if (watermark > self.applied_index) self.applied_index = watermark;
    }

    fn compactAppliedStorageIfNeeded(self: *WalReplicaState) !void {
        const retained = self.cfg.compaction_retained_entries;
        if (retained == 0 or self.applied_index <= retained) return;
        if (self.cfg.compaction_single_node_only and !(try self.hasSingleNodeConfState())) return;

        var compact_index = self.applied_index - retained;
        if (compact_index <= self.last_compacted_index) return;

        const min_interval = self.cfg.compaction_min_interval_entries;
        if (min_interval > 0 and compact_index - self.last_compacted_index < min_interval) return;

        const last_index = try self.store.storage().lastIndex();
        if (compact_index > last_index) compact_index = last_index;
        if (compact_index <= self.last_compacted_index) return;

        const first_index = try self.store.storage().firstIndex();
        if (compact_index < first_index) return;

        const started_ns = nowNs();
        var initial_state = try self.store.storage().initialState(self.alloc);
        defer initial_state.deinit(self.alloc);
        try self.store.compactTo(compact_index, initial_state.conf_state);
        self.last_compacted_index = compact_index;
        const elapsed = elapsedSince(started_ns);
        self.stats.storage_compactions += 1;
        self.stats.storage_compaction_ns += elapsed;
        self.stats.max_storage_compaction_ns = @max(self.stats.max_storage_compaction_ns, elapsed);

        try self.persistCheckpoint();
        self.delta_records_since_checkpoint = 0;
        self.delta_bytes_since_checkpoint = 0;
    }

    fn refreshLastCompactedIndex(self: *WalReplicaState) !void {
        const snapshot = try self.store.storage().snapshot(self.alloc);
        defer {
            var owned = snapshot;
            owned.deinit(self.alloc);
        }
        self.last_compacted_index = snapshot.metadata.index;
    }

    fn hasSingleNodeConfState(self: *WalReplicaState) !bool {
        var initial_state = try self.store.storage().initialState(self.alloc);
        defer initial_state.deinit(self.alloc);
        return initial_state.conf_state.voters.len == 1 and
            initial_state.conf_state.voters_outgoing.len == 0 and
            initial_state.conf_state.learners.len == 0 and
            initial_state.conf_state.learners_next.len == 0 and
            !initial_state.conf_state.auto_leave;
    }

    fn decodeWalRecord(self: *WalReplicaState, bytes: []const u8) !void {
        const record_magic = try peekRecordMagic(bytes);
        switch (record_magic) {
            magic => try self.decodeIntoStore(bytes),
            delta_magic => {
                try self.decodeDeltaIntoStore(bytes);
                self.stats.replayed_delta_records += 1;
                self.stats.replayed_delta_bytes += bytes.len;
                self.delta_records_since_checkpoint +|= 1;
                self.delta_bytes_since_checkpoint +|= bytes.len;
            },
            else => return error.InvalidReplicaState,
        }
    }

    fn peekRecordMagic(bytes: []const u8) !u32 {
        if (bytes.len < @sizeOf(u32)) return error.InvalidReplicaState;
        var buf: [4]u8 = undefined;
        @memcpy(&buf, bytes[0..4]);
        return std.mem.readInt(u32, &buf, .little);
    }

    fn decodeIntoStore(self: *WalReplicaState, bytes: []const u8) !void {
        var cursor: usize = 0;
        if (try readInt(u32, bytes, &cursor) != magic) return error.InvalidReplicaState;
        const file_version = try readInt(u32, bytes, &cursor);
        if (file_version != 1 and file_version != version) return error.UnsupportedReplicaStateVersion;

        self.store.setHardState(.{
            .current_term = try readInt(u64, bytes, &cursor),
            .voted_for = if (try readBool(bytes, &cursor)) try readInt(u64, bytes, &cursor) else null,
            .commit_index = try readInt(u64, bytes, &cursor),
        });
        self.applied_index = if (file_version >= 2)
            try readInt(u64, bytes, &cursor)
        else
            self.store.hard_state.commit_index;

        var conf_state = try decodeConfState(self.alloc, bytes, &cursor);
        defer conf_state.deinit(self.alloc);
        try self.store.setConfState(conf_state);

        const has_snapshot = try readBool(bytes, &cursor);
        if (has_snapshot) {
            const snapshot = try decodeSnapshot(self.alloc, bytes, &cursor);
            defer {
                var owned = snapshot;
                owned.deinit(self.alloc);
            }
            try self.store.applySnapshot(snapshot);
        }

        const entry_count = try readInt(u32, bytes, &cursor);
        if (entry_count > 0) {
            const entries = try self.alloc.alloc(raft_engine.core.Entry, entry_count);
            defer raft_engine.core.types.freeEntries(self.alloc, entries);
            for (entries) |*entry| entry.* = try decodeEntry(self.alloc, bytes, &cursor);
            try self.store.append(entries);
        }
    }

    fn encodeReadyDelta(self: *WalReplicaState, ready: raft_engine.core.Ready) ![]u8 {
        var buffer = std.ArrayListUnmanaged(u8).empty;
        errdefer buffer.deinit(self.alloc);

        try appendInt(u32, self.alloc, &buffer, delta_magic);
        try appendInt(u32, self.alloc, &buffer, delta_version);
        try buffer.append(self.alloc, @intFromEnum(DeltaRecordKind.ready));

        try appendBool(self.alloc, &buffer, ready.hard_state != null);
        if (ready.hard_state) |hard_state| {
            try appendInt(u64, self.alloc, &buffer, hard_state.current_term);
            try appendBool(self.alloc, &buffer, hard_state.voted_for != null);
            if (hard_state.voted_for) |voted_for| try appendInt(u64, self.alloc, &buffer, voted_for);
            try appendInt(u64, self.alloc, &buffer, hard_state.commit_index);
        }

        try appendBool(self.alloc, &buffer, ready.snapshot != null);
        if (ready.snapshot) |snapshot| try encodeSnapshot(self.alloc, &buffer, snapshot);

        try appendInt(u32, self.alloc, &buffer, @intCast(ready.entries.len));
        for (ready.entries) |entry| try encodeEntry(self.alloc, &buffer, entry);

        return try buffer.toOwnedSlice(self.alloc);
    }

    fn encodeConfStateDelta(self: *WalReplicaState) ![]u8 {
        var buffer = std.ArrayListUnmanaged(u8).empty;
        errdefer buffer.deinit(self.alloc);

        try appendInt(u32, self.alloc, &buffer, delta_magic);
        try appendInt(u32, self.alloc, &buffer, delta_version);
        try buffer.append(self.alloc, @intFromEnum(DeltaRecordKind.conf_state));

        var initial_state = try self.store.storage().initialState(self.alloc);
        defer initial_state.deinit(self.alloc);
        try encodeConfState(self.alloc, &buffer, initial_state.conf_state);

        return try buffer.toOwnedSlice(self.alloc);
    }

    fn decodeDeltaIntoStore(self: *WalReplicaState, bytes: []const u8) !void {
        var cursor: usize = 0;
        if (try readInt(u32, bytes, &cursor) != delta_magic) return error.InvalidReplicaState;
        const file_version = try readInt(u32, bytes, &cursor);
        if (file_version != delta_version) return error.UnsupportedReplicaStateVersion;

        const kind_tag = if (cursor < bytes.len) bytes[cursor] else return error.InvalidReplicaState;
        cursor += 1;
        const kind: DeltaRecordKind = switch (kind_tag) {
            @intFromEnum(DeltaRecordKind.ready) => .ready,
            @intFromEnum(DeltaRecordKind.conf_state) => .conf_state,
            else => return error.InvalidReplicaState,
        };

        switch (kind) {
            .ready => {
                const has_hard_state = try readBool(bytes, &cursor);
                if (has_hard_state) {
                    self.store.setHardState(.{
                        .current_term = try readInt(u64, bytes, &cursor),
                        .voted_for = if (try readBool(bytes, &cursor)) try readInt(u64, bytes, &cursor) else null,
                        .commit_index = try readInt(u64, bytes, &cursor),
                    });
                }

                const has_snapshot = try readBool(bytes, &cursor);
                if (has_snapshot) {
                    const snapshot = try decodeSnapshot(self.alloc, bytes, &cursor);
                    defer {
                        var owned = snapshot;
                        owned.deinit(self.alloc);
                    }
                    try self.store.applySnapshot(snapshot);
                }

                const entry_count = try readInt(u32, bytes, &cursor);
                if (entry_count > 0) {
                    const entries = try self.alloc.alloc(raft_engine.core.Entry, entry_count);
                    defer raft_engine.core.types.freeEntries(self.alloc, entries);
                    for (entries) |*entry| entry.* = try decodeEntry(self.alloc, bytes, &cursor);
                    try self.store.append(entries);
                }
            },
            .conf_state => {
                var conf_state = try decodeConfState(self.alloc, bytes, &cursor);
                defer conf_state.deinit(self.alloc);
                try self.store.setConfState(conf_state);
            },
        }
    }

    fn appendInt(comptime T: type, alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: T) !void {
        var bytes: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &bytes, value, .little);
        try out.appendSlice(alloc, &bytes);
    }

    fn appendBool(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: bool) !void {
        try out.append(alloc, @intFromBool(value));
    }

    fn appendBytes(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
        try appendInt(u32, alloc, out, @intCast(bytes.len));
        try out.appendSlice(alloc, bytes);
    }

    fn readInt(comptime T: type, bytes: []const u8, cursor: *usize) !T {
        if (cursor.* + @sizeOf(T) > bytes.len) return error.InvalidReplicaState;
        var buf: [@sizeOf(T)]u8 = undefined;
        @memcpy(&buf, bytes[cursor.* .. cursor.* + @sizeOf(T)]);
        cursor.* += @sizeOf(T);
        return std.mem.readInt(T, &buf, .little);
    }

    fn readBool(bytes: []const u8, cursor: *usize) !bool {
        if (cursor.* >= bytes.len) return error.InvalidReplicaState;
        const value = bytes[cursor.*] != 0;
        cursor.* += 1;
        return value;
    }

    fn readBytes(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) ![]u8 {
        const len = try readInt(u32, bytes, cursor);
        if (cursor.* + len > bytes.len) return error.InvalidReplicaState;
        defer cursor.* += len;
        return try alloc.dupe(u8, bytes[cursor.* .. cursor.* + len]);
    }

    fn encodeNodeList(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), nodes: []const u64) !void {
        try appendInt(u32, alloc, out, @intCast(nodes.len));
        for (nodes) |node_id| try appendInt(u64, alloc, out, node_id);
    }

    fn decodeNodeList(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) ![]u64 {
        const len = try readInt(u32, bytes, cursor);
        const out = try alloc.alloc(u64, len);
        errdefer alloc.free(out);
        for (out) |*node_id| node_id.* = try readInt(u64, bytes, cursor);
        return out;
    }

    fn encodeConfState(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), conf_state: raft_engine.core.ConfState) !void {
        try encodeNodeList(alloc, out, conf_state.voters);
        try encodeNodeList(alloc, out, conf_state.voters_outgoing);
        try encodeNodeList(alloc, out, conf_state.learners);
        try encodeNodeList(alloc, out, conf_state.learners_next);
        try appendBool(alloc, out, conf_state.auto_leave);
    }

    fn nowNs() u64 {
        return platform_time.monotonicNs();
    }

    fn elapsedSince(started_ns: u64) u64 {
        return nowNs() - started_ns;
    }

    fn writeFileAtomically(io: anytype, path: []const u8, contents: []const u8) !void {
        const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}", .{
            path,
            applied_watermark_tmp_nonce.fetchAdd(1, .monotonic),
        });
        defer std.heap.page_allocator.free(tmp_path);

        {
            var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
            defer file.close(io);
            var writer = file.writer(io, &.{});
            try writer.interface.writeAll(contents);
            try writer.end();
            try file.sync(io);
        }
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    }

    fn decodeConfState(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) !raft_engine.core.ConfState {
        return .{
            .voters = try decodeNodeList(alloc, bytes, cursor),
            .voters_outgoing = try decodeNodeList(alloc, bytes, cursor),
            .learners = try decodeNodeList(alloc, bytes, cursor),
            .learners_next = try decodeNodeList(alloc, bytes, cursor),
            .auto_leave = try readBool(bytes, cursor),
        };
    }

    fn encodeSnapshot(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), snapshot: raft_engine.core.types.Snapshot) !void {
        try appendInt(u64, alloc, out, snapshot.metadata.index);
        try appendInt(u64, alloc, out, snapshot.metadata.term);
        try encodeConfState(alloc, out, snapshot.metadata.conf_state);
        try appendBytes(alloc, out, snapshot.data);
    }

    fn decodeSnapshot(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) !raft_engine.core.types.Snapshot {
        return .{
            .metadata = .{
                .index = try readInt(u64, bytes, cursor),
                .term = try readInt(u64, bytes, cursor),
                .conf_state = try decodeConfState(alloc, bytes, cursor),
            },
            .data = try readBytes(alloc, bytes, cursor),
        };
    }

    fn encodeEntry(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), entry: raft_engine.core.Entry) !void {
        try appendInt(u64, alloc, out, entry.term);
        try appendInt(u64, alloc, out, entry.index);
        try out.append(alloc, @intFromEnum(entry.entry_type));
        try appendBytes(alloc, out, entry.data);
    }

    fn decodeEntry(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) !raft_engine.core.Entry {
        const term = try readInt(u64, bytes, cursor);
        const index = try readInt(u64, bytes, cursor);
        const entry_type_tag = if (cursor.* < bytes.len) bytes[cursor.*] else return error.InvalidReplicaState;
        cursor.* += 1;
        const data = try readBytes(alloc, bytes, cursor);
        const entry_type: raft_engine.core.types.EntryType = switch (entry_type_tag) {
            @intFromEnum(raft_engine.core.types.EntryType.normal) => .normal,
            @intFromEnum(raft_engine.core.types.EntryType.conf_change) => .conf_change,
            @intFromEnum(raft_engine.core.types.EntryType.conf_change_v2) => .conf_change_v2,
            else => return error.InvalidReplicaState,
        };
        return .{
            .term = term,
            .index = index,
            .entry_type = entry_type,
            .data = data,
        };
    }
};

test "wal replica state defaults to lsm backend" {
    try std.testing.expectEqual(wal_mod.StorageBackend.lsm, ((WalReplicaStateConfig{}).wal).resolvedBackend());
}

test "wal replica state persists ready updates across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-replica", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 177, 3);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer state.deinit();

        var ready = raft_engine.core.Ready{};
        defer {
            raft_engine.core.types.freeEntries(std.testing.allocator, @constCast(ready.entries));
        }
        ready.hard_state = .{ .current_term = 2, .voted_for = 3, .commit_index = 1 };
        ready.entries = try std.testing.allocator.dupe(raft_engine.core.Entry, &[_]raft_engine.core.Entry{
            .{ .term = 2, .index = 1, .entry_type = .normal, .data = try std.testing.allocator.dupe(u8, "wal") },
        });
        try state.groupStorage().persistReady(177, ready);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer reopened.deinit();
        var initial = try reopened.storage().initialState(std.testing.allocator);
        defer initial.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u64, 2), initial.hard_state.current_term);
        try std.testing.expectEqual(@as(?u64, 3), initial.hard_state.voted_for);
        try std.testing.expectEqual(@as(u64, 1), try reopened.storage().lastIndex());
    }
}

test "wal replica state replays committed entries when append persisted before applied watermark" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-append-before-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 178, 3);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer state.deinit();

        var ready = raft_engine.core.Ready{};
        defer {
            raft_engine.core.types.freeEntries(std.testing.allocator, @constCast(ready.entries));
        }
        ready.hard_state = .{ .current_term = 3, .voted_for = 1, .commit_index = 2 };
        ready.entries = try std.testing.allocator.dupe(raft_engine.core.Entry, &[_]raft_engine.core.Entry{
            .{ .term = 3, .index = 1, .data = try std.testing.allocator.dupe(u8, "one") },
            .{ .term = 3, .index = 2, .data = try std.testing.allocator.dupe(u8, "two") },
        });
        try state.groupStorage().persistReady(178, ready);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer reopened.deinit();
        try std.testing.expectEqual(@as(u64, 0), reopened.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 178,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
        try std.testing.expectEqual(@as(u64, 1), rd.committed_entries[0].index);
        try std.testing.expectEqual(@as(u64, 2), rd.committed_entries[1].index);
    }
}

test "wal replica state persists snapshots across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-replica", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 188, 4);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer state.deinit();

        var ready = raft_engine.core.Ready{};
        defer {
            if (ready.snapshot) |*snapshot| snapshot.deinit(std.testing.allocator);
        }
        ready.snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 4,
                .conf_state = .{
                    .voters = try std.testing.allocator.dupe(u64, &.{ 1, 2 }),
                },
            },
            .data = try std.testing.allocator.dupe(u8, "snap"),
        };
        try state.groupStorage().persistReady(188, ready);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer reopened.deinit();
        const snapshot = try reopened.storage().snapshot(std.testing.allocator);
        defer {
            var owned = snapshot;
            owned.deinit(std.testing.allocator);
        }
        try std.testing.expectEqual(@as(u64, 11), snapshot.metadata.index);
        try std.testing.expectEqualStrings("snap", snapshot.data);
    }
}

test "wal replica state persists applied watermark in sidecar and replays only unapplied suffix" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-applied-replay", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 189, 5);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer state.deinit();

        const snapshot_voters = try std.testing.allocator.dupe(u64, &.{1});
        defer std.testing.allocator.free(snapshot_voters);
        const snapshot_data = try std.testing.allocator.dupe(u8, "wal-snap");
        defer std.testing.allocator.free(snapshot_data);
        const ten_data = try std.testing.allocator.dupe(u8, "ten");
        defer std.testing.allocator.free(ten_data);
        const eleven_data = try std.testing.allocator.dupe(u8, "eleven");
        defer std.testing.allocator.free(eleven_data);

        try state.groupStorage().persistReady(189, .{
            .hard_state = .{ .current_term = 4, .voted_for = 1, .commit_index = 11 },
            .snapshot = .{
                .metadata = .{
                    .index = 9,
                    .term = 4,
                    .conf_state = .{ .voters = snapshot_voters },
                },
                .data = snapshot_data,
            },
            .entries = &.{
                .{ .term = 4, .index = 10, .data = ten_data },
                .{ .term = 4, .index = 11, .data = eleven_data },
            },
        });
        try state.setAppliedIndex(10);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{});
        defer reopened.deinit();
        try std.testing.expectEqual(@as(u64, 10), reopened.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 189,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
        try std.testing.expectEqual(@as(u64, 11), rd.committed_entries[0].index);
        try std.testing.expectEqualStrings("eleven", rd.committed_entries[0].data);
    }
}

test "wal replica state tracks persist reasons separately" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-stats", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 190, 6);
    defer layout.deinit(std.testing.allocator);

    var state = try WalReplicaState.init(std.testing.allocator, layout, .{});
    defer state.deinit();

    const one_data = try std.testing.allocator.dupe(u8, "one");
    defer std.testing.allocator.free(one_data);
    try state.groupStorage().persistReady(190, .{
        .hard_state = .{ .current_term = 2, .voted_for = 1, .commit_index = 1 },
        .entries = &.{
            .{ .term = 2, .index = 1, .data = one_data },
        },
    });
    try state.setAppliedIndex(1);

    const stats = state.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.persist_ready_calls);
    try std.testing.expectEqual(@as(u64, 1), stats.ready_persist_calls);
    try std.testing.expectEqual(@as(u64, 1), stats.applied_index_updates);
    try std.testing.expectEqual(@as(u64, 1), stats.applied_index_persist_calls);
    try std.testing.expect(stats.persist_ns > 0);
    try std.testing.expect(stats.encode_ns > 0);
    try std.testing.expect(stats.wal_append_ns > 0);
    try std.testing.expect(stats.encoded_bytes > 0);
    try std.testing.expect(stats.applied_watermark_persist_ns > 0);
    try std.testing.expectEqual(@as(u64, 16), stats.applied_watermark_bytes);
}

test "wal replica state checkpoints and compacts delta records when replay debt crosses threshold" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-checkpoint-compact", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 191, 7);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 2,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer state.deinit();

        const one_data = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(one_data);
        try state.groupStorage().persistReady(191, .{
            .hard_state = .{ .current_term = 2, .voted_for = 1, .commit_index = 1 },
            .entries = &.{.{ .term = 2, .index = 1, .data = one_data }},
        });
        try state.setAppliedIndex(1);

        const two_data = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(two_data);
        try state.groupStorage().persistReady(191, .{
            .hard_state = .{ .current_term = 2, .voted_for = 1, .commit_index = 2 },
            .entries = &.{.{ .term = 2, .index = 2, .data = two_data }},
        });
        try state.setAppliedIndex(2);

        const stats = state.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 1), stats.checkpoint_persist_calls);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_records);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_bytes);

        const wal_entries = try state.wal.iterateFrom(std.testing.allocator, 1);
        defer {
            for (wal_entries) |entry| std.testing.allocator.free(@constCast(entry.data));
            std.testing.allocator.free(wal_entries);
        }
        try std.testing.expectEqual(@as(usize, 1), wal_entries.len);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 2,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer reopened.deinit();
        try std.testing.expectEqual(@as(u64, 2), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 2), try reopened.storage().lastIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 191,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(!raw.hasReady());
    }
}

test "wal replica state checkpoints when replay debt crosses byte threshold" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-checkpoint-bytes", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 195, 11);
    defer layout.deinit(std.testing.allocator);

    var state = try WalReplicaState.init(std.testing.allocator, layout, .{
        .checkpoint_replay_records_threshold = 0,
        .checkpoint_replay_bytes_threshold = 1,
    });
    defer state.deinit();

    const payload = try std.testing.allocator.dupe(u8, "payload-large-enough-for-threshold");
    defer std.testing.allocator.free(payload);
    try state.groupStorage().persistReady(195, .{
        .hard_state = .{ .current_term = 2, .voted_for = 1, .commit_index = 1 },
        .entries = &.{.{ .term = 2, .index = 1, .data = payload }},
    });
    try state.setAppliedIndex(1);

    const stats = state.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.checkpoint_persist_calls);
    try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_records);
    try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_bytes);
}

test "wal replica state keeps replay debt bounded across repeated checkpoints" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-bounded-replay-debt", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 197, 13);
    defer layout.deinit(std.testing.allocator);

    const cfg = WalReplicaStateConfig{
        .checkpoint_replay_records_threshold = 4,
        .checkpoint_replay_bytes_threshold = 0,
    };

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer state.deinit();

        var index: u64 = 1;
        while (index <= 20) : (index += 1) {
            const payload = try std.fmt.allocPrint(std.testing.allocator, "entry-{d}", .{index});
            defer std.testing.allocator.free(payload);
            try state.groupStorage().persistReady(197, .{
                .hard_state = .{ .current_term = 7, .voted_for = 1, .commit_index = index },
                .entries = &.{.{ .term = 7, .index = index, .data = payload }},
            });
            try state.setAppliedIndex(index);
        }

        const stats = state.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 5), stats.checkpoint_persist_calls);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_records);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_bytes);

        const wal_entries = try state.wal.iterateFrom(std.testing.allocator, 1);
        defer {
            for (wal_entries) |entry| std.testing.allocator.free(@constCast(entry.data));
            std.testing.allocator.free(wal_entries);
        }
        try std.testing.expect(wal_entries.len <= 2);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer reopened.deinit();

        try std.testing.expectEqual(@as(u64, 20), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 20), try reopened.storage().lastIndex());

        const stats = reopened.statsSnapshot();
        try std.testing.expect(stats.replayed_delta_records <= 1);
        try std.testing.expect(stats.replay_debt_records <= 1);
    }
}

test "wal replica state batches applied watermark persistence between durable checkpoints" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-batched-watermark", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 199, 15);
    defer layout.deinit(std.testing.allocator);

    const cfg = WalReplicaStateConfig{
        .checkpoint_replay_records_threshold = 0,
        .checkpoint_replay_bytes_threshold = 0,
        .applied_watermark_persist_interval = 4,
        .compaction_retained_entries = 0,
    };

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer state.deinit();

        var index: u64 = 1;
        while (index <= 3) : (index += 1) {
            const payload = try std.fmt.allocPrint(std.testing.allocator, "entry-{d}", .{index});
            defer std.testing.allocator.free(payload);
            try state.groupStorage().persistReady(199, .{
                .hard_state = .{ .current_term = 9, .voted_for = 1, .commit_index = index },
                .entries = &.{.{ .term = 9, .index = index, .data = payload }},
            });
            try state.setAppliedIndex(index);
        }

        const stats = state.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 3), stats.applied_index_updates);
        try std.testing.expectEqual(@as(u64, 1), stats.applied_index_persist_calls);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer reopened.deinit();

        try std.testing.expectEqual(@as(u64, 1), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 3), try reopened.storage().lastIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 199,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
        try std.testing.expectEqual(@as(u64, 2), rd.committed_entries[0].index);
        try std.testing.expectEqual(@as(u64, 3), rd.committed_entries[1].index);
    }
}

test "wal replica state compacts applied storage and checkpoints compacted image" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-applied-compaction", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 200, 16);
    defer layout.deinit(std.testing.allocator);

    const cfg = WalReplicaStateConfig{
        .checkpoint_replay_records_threshold = 0,
        .checkpoint_replay_bytes_threshold = 0,
        .applied_watermark_persist_interval = 64,
        .compaction_retained_entries = 2,
        .compaction_min_interval_entries = 1,
        .compaction_single_node_only = false,
    };

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer state.deinit();

        var index: u64 = 1;
        while (index <= 8) : (index += 1) {
            const payload = try std.fmt.allocPrint(std.testing.allocator, "entry-{d}", .{index});
            defer std.testing.allocator.free(payload);
            try state.groupStorage().persistReady(200, .{
                .hard_state = .{ .current_term = 10, .voted_for = 1, .commit_index = index },
                .entries = &.{.{ .term = 10, .index = index, .data = payload }},
            });
            try state.setAppliedIndex(index);
        }

        const stats = state.statsSnapshot();
        try std.testing.expect(stats.storage_compactions > 0);
        try std.testing.expectEqual(@as(u64, 6), stats.last_compacted_index);
        try std.testing.expectEqual(@as(u64, 6), stats.checkpoint_persist_calls);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_records);
        try std.testing.expectEqual(@as(u64, 7), try state.storage().firstIndex());
        try std.testing.expectEqual(@as(u64, 8), try state.storage().lastIndex());
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer reopened.deinit();

        try std.testing.expectEqual(@as(u64, 8), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 7), try reopened.storage().firstIndex());
        try std.testing.expectEqual(@as(u64, 8), try reopened.storage().lastIndex());

        const stats = reopened.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 6), stats.last_compacted_index);
        try std.testing.expectEqual(@as(u64, 0), stats.replayed_delta_records);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_records);
    }
}

test "wal replica state reopens from full-image checkpoint plus newer delta tail" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-mixed-checkpoint-tail", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 192, 8);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer state.deinit();

        const one_data = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(one_data);
        try state.groupStorage().persistReady(192, .{
            .hard_state = .{ .current_term = 3, .voted_for = 1, .commit_index = 1 },
            .entries = &.{.{ .term = 3, .index = 1, .data = one_data }},
        });
        try state.setAppliedIndex(1);

        // Simulate an older full-image checkpoint written before the newer delta tail.
        try state.persist(.checkpoint);
        state.delta_records_since_checkpoint = 0;
        state.delta_bytes_since_checkpoint = 0;

        const two_data = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(two_data);
        try state.groupStorage().persistReady(192, .{
            .hard_state = .{ .current_term = 3, .voted_for = 1, .commit_index = 2 },
            .entries = &.{.{ .term = 3, .index = 2, .data = two_data }},
        });
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer reopened.deinit();

        try std.testing.expectEqual(@as(u64, 1), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 2), try reopened.storage().lastIndex());

        const stats = reopened.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 1), stats.replayed_delta_records);
        try std.testing.expect(stats.replayed_delta_bytes > 0);
        try std.testing.expectEqual(@as(u64, 1), stats.replay_debt_records);

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 192,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
        try std.testing.expectEqual(@as(u64, 2), rd.committed_entries[0].index);
        try std.testing.expectEqualStrings("two", rd.committed_entries[0].data);
    }
}

test "wal replica state reopens from checkpoint when applied watermark sidecar is missing" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-checkpoint-without-watermark", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 198, 14);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer state.deinit();

        const one_data = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(one_data);
        const two_data = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(two_data);

        try state.groupStorage().persistReady(198, .{
            .hard_state = .{ .current_term = 8, .voted_for = 1, .commit_index = 2 },
            .entries = &.{
                .{ .term = 8, .index = 1, .data = one_data },
                .{ .term = 8, .index = 2, .data = two_data },
            },
        });
        try state.setAppliedIndex(2);
        try state.persist(.checkpoint);
        state.delta_records_since_checkpoint = 0;
        state.delta_bytes_since_checkpoint = 0;
        try std.Io.Dir.cwd().deleteFile(state.io_impl.io(), state.applied_watermark_path);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer reopened.deinit();

        try std.testing.expectEqual(@as(u64, 2), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 2), try reopened.storage().lastIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 198,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(!raw.hasReady());
    }
}

test "wal replica state reopens with applied watermark newer than checkpoint" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-watermark-newer-than-checkpoint", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 196, 12);
    defer layout.deinit(std.testing.allocator);

    const cfg = WalReplicaStateConfig{
        .checkpoint_replay_records_threshold = 0,
        .checkpoint_replay_bytes_threshold = 0,
        .applied_watermark_persist_interval = 1,
    };

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer state.deinit();

        const one_data = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(one_data);
        const two_data = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(two_data);

        try state.groupStorage().persistReady(196, .{
            .hard_state = .{ .current_term = 6, .voted_for = 1, .commit_index = 2 },
            .entries = &.{
                .{ .term = 6, .index = 1, .data = one_data },
                .{ .term = 6, .index = 2, .data = two_data },
            },
        });
        try state.setAppliedIndex(1);
        try state.persist(.checkpoint);
        state.delta_records_since_checkpoint = 0;
        state.delta_bytes_since_checkpoint = 0;

        try state.setAppliedIndex(2);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, cfg);
        defer reopened.deinit();

        try std.testing.expectEqual(@as(u64, 2), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 2), try reopened.storage().lastIndex());

        const stats = reopened.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 0), stats.replayed_delta_records);
        try std.testing.expectEqual(@as(u64, 0), stats.replay_debt_records);

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 196,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(!raw.hasReady());
    }
}

test "wal replica state stats report replay debt without checkpoint" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-replay-debt-stats", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 193, 9);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer state.deinit();

        const one_data = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(one_data);
        try state.groupStorage().persistReady(193, .{
            .hard_state = .{ .current_term = 4, .voted_for = 1, .commit_index = 1 },
            .entries = &.{.{ .term = 4, .index = 1, .data = one_data }},
        });

        const two_data = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(two_data);
        try state.groupStorage().persistReady(193, .{
            .hard_state = .{ .current_term = 4, .voted_for = 1, .commit_index = 2 },
            .entries = &.{.{ .term = 4, .index = 2, .data = two_data }},
        });

        const live_stats = state.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 2), live_stats.replay_debt_records);
        try std.testing.expect(live_stats.replay_debt_bytes > 0);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer reopened.deinit();

        const stats = reopened.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 2), stats.replayed_delta_records);
        try std.testing.expect(stats.replayed_delta_bytes > 0);
        try std.testing.expectEqual(@as(u64, 2), stats.replay_debt_records);
        try std.testing.expect(stats.replay_debt_bytes > 0);
    }
}

test "wal replica state flush for shutdown checkpoints outstanding replay debt" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-shutdown-flush", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 194, 10);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer state.deinit();

        const one_data = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(one_data);
        try state.groupStorage().persistReady(194, .{
            .hard_state = .{ .current_term = 5, .voted_for = 1, .commit_index = 1 },
            .entries = &.{.{ .term = 5, .index = 1, .data = one_data }},
        });
        try state.setAppliedIndex(1);

        const two_data = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(two_data);
        try state.groupStorage().persistReady(194, .{
            .hard_state = .{ .current_term = 5, .voted_for = 1, .commit_index = 2 },
            .entries = &.{.{ .term = 5, .index = 2, .data = two_data }},
        });
        try state.setAppliedIndex(2);

        const before = state.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 2), before.replay_debt_records);

        try state.flushForShutdown();

        const after = state.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 0), after.replay_debt_records);
        try std.testing.expect(after.checkpoint_persist_calls >= 1);
    }

    {
        var reopened = try WalReplicaState.init(std.testing.allocator, layout, .{
            .checkpoint_replay_records_threshold = 0,
            .checkpoint_replay_bytes_threshold = 0,
        });
        defer reopened.deinit();

        try std.testing.expectEqual(@as(u64, 2), reopened.appliedIndex());
        try std.testing.expectEqual(@as(u64, 2), try reopened.storage().lastIndex());

        const stats = reopened.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 0), stats.replayed_delta_records);

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 194,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(!raw.hasReady());
    }
}
