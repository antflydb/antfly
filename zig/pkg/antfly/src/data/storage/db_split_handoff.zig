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
const data_store = @import("raft_apply_store.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const shard_state_store = @import("shard_state_store.zig");
const internal_keys = @import("../../storage/internal_keys.zig");
const shard_mod = @import("../../storage/shard.zig");
const db_mod = @import("../../storage/db/db.zig");
const doc_identity = @import("../../storage/db/doc_identity.zig");
const db_types = @import("../../storage/db/types.zig");
const range_state = @import("../../storage/db/range_state.zig");
const raft_state_machine = @import("../../raft/state_machine/mod.zig");
const range_transition = @import("range_transition.zig");
const lmdb = @import("../../storage/lmdb.zig");

pub const DestinationConfig = struct {
    root_dir: []const u8,
    db: db_mod.OpenOptions = .{},
};

pub const BorrowedDestinationDb = struct {
    db: *db_mod.DB,
    ctx: *anyopaque,
    release_fn: *const fn (*anyopaque) void,

    pub fn release(self: BorrowedDestinationDb) void {
        self.release_fn(self.ctx);
    }
};

pub const SyncConfig = struct {
    transition_id: u64 = 1,
    attempt_epoch: u64 = 1,
    source_root_dir: []const u8,
    dest_root_dir: []const u8,
    source_group_id: u64,
    dest_group_id: u64,
    source: data_store.RaftApplyStoreConfig = .{ .root_dir = "" },
    source_store: ?*data_store.RaftApplyStore = null,
    dest: DestinationConfig = .{ .root_dir = "" },
    dest_db: ?*db_mod.DB = null,
    /// Optional durable source-side acknowledgement used by distributed
    /// handoff, where the source leader need not host a destination replica.
    progress_db: ?*db_mod.DB = null,
    dest_lease: ?BorrowedDestinationDb = null,
};

pub fn observeSplitStatus(alloc: std.mem.Allocator, cfg: SyncConfig) !SplitSyncStatus {
    var owned_source: ?data_store.RaftApplyStore = null;
    defer if (owned_source) |*source| source.deinit();
    const source = cfg.source_store orelse source: {
        owned_source = try data_store.RaftApplyStore.init(alloc, cfg.source);
        break :source &owned_source.?;
    };
    const source_state = try source.currentSplitState(alloc, cfg.source_group_id);
    defer if (source_state) |state| shard_state_store.freeSplitState(alloc, state);
    if (source_state) |state| try validateActiveSplitDestination(state, cfg.transition_id, cfg.attempt_epoch, cfg.dest_group_id);
    const source_terminal = try source.currentSplitTerminal(alloc, cfg.source_group_id);
    defer if (source_terminal) |terminal| shard_state_store.freeSplitTerminal(alloc, terminal);
    const matching_terminal = if (source_state == null and source_terminal != null) terminal: {
        const terminal = source_terminal.?;
        if (cfg.attempt_epoch < terminal.attempt_epoch) return error.StaleSplitAttempt;
        if (cfg.attempt_epoch > terminal.attempt_epoch) break :terminal null;
        try shard_state_store.validateSplitTerminalIdentity(
            terminal,
            cfg.transition_id,
            cfg.attempt_epoch,
            cfg.dest_group_id,
            null,
        );
        break :terminal terminal.outcome;
    } else null;

    var owned_dest: ?db_mod.DB = null;
    defer if (owned_dest) |*dest| dest.close();
    var dest_db = cfg.dest_db;
    if (cfg.progress_db == null and dest_db == null) {
        var dest_opts = cfg.dest.db;
        dest_opts.open_mode = .status_only;
        dest_opts.start_index_workers = false;
        owned_dest = try db_mod.DB.open(alloc, cfg.dest_root_dir, dest_opts);
        dest_db = &owned_dest.?;
    }
    if (dest_db) |db| try validateConfiguredDestinationIdentityNamespace(cfg.dest.db.identity_namespace, db);

    const progress_db = cfg.progress_db orelse dest_db orelse return error.MissingSplitProgressStore;
    const bootstrap_marker = try progress_db.getSplitBootstrapMarker(alloc);
    const bootstrapped = if (bootstrap_marker) |marker|
        marker.transition_id == cfg.transition_id and
            marker.attempt_epoch == cfg.attempt_epoch and
            marker.source_group_id == cfg.source_group_id and
            marker.destination_group_id == cfg.dest_group_id and
            marker.bootstrap_complete
    else
        false;
    const source_seq = try source.currentSplitDeltaSequence(alloc, cfg.source_group_id);
    const dest_seq = try progress_db.getSplitDeltaFinalSeq(alloc);
    if (matching_terminal) |outcome| {
        return range_transition.completedSplitStatus(outcome == .finalized, source_seq, dest_seq);
    }
    // No active source state is the prepare trigger for a successor. The
    // controller uses the record's split coordinates to issue the fenced
    // prepare command; the source epoch rejects stale predecessors.
    const source_phase = if (source_state) |state| state.phase else null;
    const derived = range_transition.deriveSplitStatus(source_phase, bootstrapped, source_seq, dest_seq);
    return .{
        .phase = derived.phase,
        .source_split_phase = derived.source_split_phase orelse switch (derived.phase) {
            .finalized, .rolled_back => .none,
            else => null,
        },
        .bootstrapped = derived.bootstrapped,
        .replay_required = derived.replay_required,
        .replay_caught_up = derived.replay_caught_up,
        .cutover_ready = derived.cutover_ready,
        .destination_ready_for_reads = derived.destination_ready_for_reads,
        .source_delta_sequence = derived.source_delta_sequence,
        .dest_delta_sequence = derived.dest_delta_sequence,
    };
}

fn validateConfiguredDestinationIdentityNamespace(expected: ?doc_identity.Namespace, db: *const db_mod.DB) !void {
    const namespace = expected orelse return;
    if (!db.core.identity_namespace.eql(namespace)) return error.DocIdentityNamespaceMismatch;
}

fn validateActiveSplitDestination(state: shard_state_store.AppliedSplitState, transition_id: u64, attempt_epoch: u64, destination_group_id: u64) !void {
    if (state.phase == .none) return;
    try shard_state_store.validateSplitIdentity(state, transition_id, attempt_epoch, destination_group_id, null);
}

pub const MergeConfig = struct {
    donor_root_dir: []const u8,
    receiver_root_dir: []const u8,
    donor_group_id: u64,
    receiver_group_id: u64,
    donor: data_store.RaftApplyStoreConfig = .{ .root_dir = "" },
    donor_store: ?*data_store.RaftApplyStore = null,
    receiver: DestinationConfig = .{ .root_dir = "" },
    receiver_db: ?*db_mod.DB = null,
    receiver_lease: ?BorrowedDestinationDb = null,
    receiver_identity_reassignment_namespace: ?doc_identity.Namespace = null,
};

const merge_state_key = "raftmerge:state";

const MergeLifecyclePhase = enum(u8) {
    none = 0,
    accepting = 1,
    finalized = 2,
    rolling_back = 3,
    rolled_back = 4,
};

const PersistedMergeState = struct {
    donor_group_id: u64,
    receiver_group_id: u64,
    phase: MergeLifecyclePhase,
    receiver_base_range: db_types.ByteRange,
    allow_doc_identity_reassignment: bool = false,
    receiver_identity_reassignment_namespace: ?doc_identity.Namespace = null,
};

pub const SplitTransitionPhase = range_transition.TransitionPhase;

pub const Destination = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    db: *db_mod.DB,
    owned_db: ?*db_mod.DB,
    borrowed_lease: ?BorrowedDestinationDb,

    pub fn init(alloc: std.mem.Allocator, cfg: DestinationConfig) !Destination {
        return try initWithCreateRoot(alloc, cfg, true);
    }

    fn initWithCreateRoot(alloc: std.mem.Allocator, cfg: DestinationConfig, create_root: bool) !Destination {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();

        const root_dir = try alloc.dupe(u8, cfg.root_dir);
        errdefer alloc.free(root_dir);
        if (create_root) {
            try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        } else {
            try std.Io.Dir.cwd().access(io_impl.io(), root_dir, .{});
        }

        const db = try alloc.create(db_mod.DB);
        errdefer alloc.destroy(db);
        db.* = try db_mod.DB.open(alloc, root_dir, cfg.db);
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .root_dir = root_dir,
            .db = db,
            .owned_db = db,
            .borrowed_lease = null,
        };
    }

    pub fn initBorrowed(alloc: std.mem.Allocator, root_dir_raw: []const u8, lease: BorrowedDestinationDb) !Destination {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .root_dir = try alloc.dupe(u8, root_dir_raw),
            .db = lease.db,
            .owned_db = null,
            .borrowed_lease = lease,
        };
    }

    pub fn initReadOnly(alloc: std.mem.Allocator, root_dir_raw: []const u8) !Destination {
        return try initReadOnlyWithOptions(alloc, root_dir_raw, .{});
    }

    pub fn initReadOnlyWithOptions(alloc: std.mem.Allocator, root_dir_raw: []const u8, db_options: db_mod.OpenOptions) !Destination {
        return try initWithReadOnlyMode(alloc, root_dir_raw, db_options, .status_only);
    }

    pub fn initQueryReadOnlyWithOptions(alloc: std.mem.Allocator, root_dir_raw: []const u8, db_options: db_mod.OpenOptions) !Destination {
        return try initWithReadOnlyMode(alloc, root_dir_raw, db_options, .query_readonly);
    }

    fn initWithReadOnlyMode(
        alloc: std.mem.Allocator,
        root_dir_raw: []const u8,
        db_options: db_mod.OpenOptions,
        mode: db_mod.OpenOptions.OpenMode,
    ) !Destination {
        var read_only_options = db_options;
        read_only_options.open_mode = mode;
        read_only_options.start_index_workers = false;
        read_only_options.start_optional_runtimes = false;
        return try initWithCreateRoot(
            alloc,
            .{
                .root_dir = root_dir_raw,
                .db = read_only_options,
            },
            false,
        );
    }

    pub fn deinit(self: *Destination) void {
        if (self.owned_db) |owned| {
            owned.close();
            self.alloc.destroy(owned);
        }
        if (self.borrowed_lease) |lease| lease.release();
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn applyHandoff(
        self: *Destination,
        alloc: std.mem.Allocator,
        handoff: data_store.SplitHandoff,
        marker: range_state.SplitBootstrapMarker,
    ) !bool {
        if (!marker.bootstrap_complete) return error.InvalidSplitBootstrapMarker;

        const writes = try alloc.alloc(db_types.BatchWrite, handoff.entries.len);
        defer alloc.free(writes);
        for (handoff.entries, 0..) |entry, i| {
            writes[i] = .{
                .key = entry.key,
                .value = entry.value,
            };
        }
        var reservation = marker;
        reservation.bootstrap_complete = false;
        const replaced = try self.db.replaceSplitBootstrap(
            alloc,
            handoff.byte_range,
            writes,
            handoff.base_delta_sequence,
            reservation,
        );
        const completed = try self.db.completeSplitBootstrap(
            alloc,
            handoff.byte_range,
            handoff.base_delta_sequence,
            marker,
        );
        return replaced or completed;
    }

    pub fn applyMergeBootstrap(
        self: *Destination,
        alloc: std.mem.Allocator,
        donor_range: db_types.ByteRange,
        donor_entries: []const shard_state_store.AppliedDataKV,
        donor_applied_index: u64,
    ) !void {
        const current_range = self.db.getRange();
        try self.db.updateRange(mergeRanges(current_range, donor_range));

        if (donor_entries.len > 0) {
            const writes = try alloc.alloc(db_types.BatchWrite, donor_entries.len);
            defer alloc.free(writes);
            for (donor_entries, 0..) |entry, i| {
                writes[i] = .{
                    .key = entry.key,
                    .value = entry.value,
                };
            }
            try self.db.batch(.{ .writes = writes });
        }
        try self.db.setSplitDeltaFinalSeq(donor_applied_index);
    }

    pub fn applyDeltas(self: *Destination, alloc: std.mem.Allocator, deltas: []const shard_mod.SplitDelta) !void {
        const current_range = self.db.getRange();
        var max_seq = try self.db.getSplitDeltaFinalSeq(alloc);

        for (deltas) |delta| {
            var write_list = std.ArrayListUnmanaged(db_types.BatchWrite).empty;
            defer write_list.deinit(alloc);
            var owned_write_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_write_keys.items) |key| alloc.free(key);
                owned_write_keys.deinit(alloc);
            }
            var delete_list = std.ArrayListUnmanaged([]const u8).empty;
            defer delete_list.deinit(alloc);

            for (delta.writes) |write| {
                const key = try decodeDocumentKeyAlloc(alloc, write.key);
                if (!current_range.contains(key)) {
                    alloc.free(key);
                    continue;
                }
                try owned_write_keys.append(alloc, key);
                try write_list.append(alloc, .{
                    .key = key,
                    .value = write.value,
                });
            }

            for (delta.deletes) |delete_key| {
                const key = try decodeDocumentKeyAlloc(alloc, delete_key);
                defer alloc.free(key);
                if (!current_range.contains(key)) continue;
                try delete_list.append(alloc, try alloc.dupe(u8, key));
            }

            defer for (delete_list.items) |key| alloc.free(@constCast(key));

            try self.db.batch(.{
                .writes = write_list.items,
                .deletes = delete_list.items,
            });
            max_seq = @max(max_seq, delta.sequence);
        }

        try self.db.setSplitDeltaFinalSeq(max_seq);
    }

    pub fn get(self: *Destination, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
        return try self.db.get(alloc, key);
    }

    pub fn getRange(self: *Destination) db_types.ByteRange {
        return self.db.getRange();
    }

    pub fn appliedDeltaSequence(self: *Destination, alloc: std.mem.Allocator) !u64 {
        return try self.db.getSplitDeltaFinalSeq(alloc);
    }

    pub fn loadMergeState(self: *Destination, alloc: std.mem.Allocator) !?PersistedMergeState {
        const raw = (try self.db.core.getStoreValue(alloc, merge_state_key)) orelse return null;
        defer alloc.free(raw);
        return try decodeMergeStateAlloc(alloc, raw);
    }

    pub fn saveMergeState(self: *Destination, alloc: std.mem.Allocator, state: PersistedMergeState) !void {
        var encoded = std.ArrayListUnmanaged(u8).empty;
        defer encoded.deinit(alloc);
        try encodeMergeState(&encoded, alloc, state);
        try self.db.core.putStoreBatch(&.{
            .{ .key = merge_state_key, .value = encoded.items },
        }, &.{});
    }

    pub fn deleteDocsInRange(self: *Destination, alloc: std.mem.Allocator, byte_range: db_types.ByteRange) !void {
        const lower = try internal_keys.documentRangeLowerAlloc(alloc, byte_range.start);
        defer alloc.free(lower);
        const upper = if (byte_range.end.len > 0) try internal_keys.documentRangeUpperAlloc(alloc, byte_range.end) else null;
        defer if (upper) |buf| alloc.free(buf);

        const docs = try self.db.core.scanStoreRange(alloc, lower, if (upper) |buf| buf else "");
        defer {
            for (docs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(docs);
        }

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(@constCast(key));
            deletes.deinit(alloc);
        }

        for (docs) |kv| {
            const doc_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, kv.key)) orelse continue;
            errdefer alloc.free(doc_key);
            try deletes.append(alloc, doc_key);
        }

        if (deletes.items.len > 0) {
            try self.db.batch(.{
                .deletes = deletes.items,
            });
        }
    }

    pub fn applyMergeReplay(
        self: *Destination,
        alloc: std.mem.Allocator,
        donor_range: db_types.ByteRange,
        operations: []const ReplayOperation,
        donor_applied_index: u64,
    ) !void {
        var writes = std.ArrayListUnmanaged(db_types.BatchWrite).empty;
        defer writes.deinit(alloc);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(@constCast(key));
            deletes.deinit(alloc);
        }

        for (operations) |op| switch (op) {
            .put => |put| {
                if (!donor_range.contains(put.key)) continue;
                try writes.append(alloc, .{
                    .key = put.key,
                    .value = put.value,
                });
            },
            .delete => |key| {
                if (!donor_range.contains(key)) continue;
                try deletes.append(alloc, try alloc.dupe(u8, key));
            },
        };

        if (writes.items.len > 0 or deletes.items.len > 0) {
            try self.db.batch(.{
                .writes = writes.items,
                .deletes = deletes.items,
            });
        }
        try self.db.setSplitDeltaFinalSeq(donor_applied_index);
    }
};

pub const SyncCoordinator = struct {
    alloc: std.mem.Allocator,
    transition_id: u64,
    attempt_epoch: u64,
    source_root_dir: []u8,
    dest_root_dir: []u8,
    source_group_id: u64,
    dest_group_id: u64,
    source_cfg: data_store.RaftApplyStoreConfig,
    dest_cfg: DestinationConfig,
    source: *data_store.RaftApplyStore,
    source_owned: bool,
    source_open: bool,
    dest: Destination,
    dest_open: bool,

    pub fn init(alloc: std.mem.Allocator, cfg: SyncConfig) !SyncCoordinator {
        const source_root_dir = try alloc.dupe(u8, cfg.source_root_dir);
        var source_root_dir_owned = true;
        errdefer if (source_root_dir_owned) alloc.free(source_root_dir);
        const dest_root_dir = try alloc.dupe(u8, cfg.dest_root_dir);
        errdefer alloc.free(dest_root_dir);

        var source_cfg = cfg.source;
        source_cfg.root_dir = source_root_dir;
        source_root_dir_owned = false;
        errdefer freeConfig(alloc, source_cfg);

        var dest_cfg = cfg.dest;
        dest_cfg.root_dir = dest_root_dir;

        var source_owned = false;
        const source = cfg.source_store orelse source: {
            const owned = try alloc.create(data_store.RaftApplyStore);
            errdefer alloc.destroy(owned);
            owned.* = try data_store.RaftApplyStore.init(alloc, source_cfg);
            source_owned = true;
            break :source owned;
        };
        errdefer if (source_owned) {
            source.deinit();
            alloc.destroy(source);
        };
        const source_state = try source.currentSplitState(alloc, cfg.source_group_id);
        defer if (source_state) |state| shard_state_store.freeSplitState(alloc, state);
        if (source_state) |state| try validateActiveSplitDestination(state, cfg.transition_id, cfg.attempt_epoch, cfg.dest_group_id);

        var dest = if (cfg.dest_lease) |lease|
            try Destination.initBorrowed(alloc, dest_root_dir, lease)
        else
            try Destination.init(alloc, dest_cfg);
        errdefer dest.deinit();

        return .{
            .alloc = alloc,
            .transition_id = cfg.transition_id,
            .attempt_epoch = cfg.attempt_epoch,
            .source_root_dir = source_root_dir,
            .dest_root_dir = dest_root_dir,
            .source_group_id = cfg.source_group_id,
            .dest_group_id = cfg.dest_group_id,
            .source_cfg = source_cfg,
            .dest_cfg = dest_cfg,
            .source = source,
            .source_owned = source_owned,
            .source_open = true,
            .dest = dest,
            .dest_open = true,
        };
    }

    pub fn deinit(self: *SyncCoordinator) void {
        if (self.dest_open) self.dest.deinit();
        if (self.source_owned) {
            if (self.source_open) self.source.deinit();
            self.alloc.destroy(self.source);
        }
        freeConfig(self.alloc, self.source_cfg);
        self.alloc.free(self.dest_root_dir);
        self.* = undefined;
    }

    pub fn reopenSource(self: *SyncCoordinator) !void {
        try self.reopenSourceWithAllocator(self.alloc);
    }

    fn reopenSourceWithAllocator(self: *SyncCoordinator, reopen_alloc: std.mem.Allocator) !void {
        if (!self.source_owned) return error.BorrowedSplitSourceStore;
        _ = try self.sourceStore();
        self.source.deinit();
        self.source_open = false;
        self.source.* = try data_store.RaftApplyStore.init(reopen_alloc, self.source_cfg);
        self.source_open = true;
    }

    pub fn reopenDestination(self: *SyncCoordinator) !void {
        try self.reopenDestinationWithAllocator(self.alloc);
    }

    fn reopenDestinationWithAllocator(self: *SyncCoordinator, reopen_alloc: std.mem.Allocator) !void {
        const source = try self.sourceStore();
        const dest = try self.destination();
        if (dest.owned_db == null) return error.BorrowedSplitDestinationDb;
        const source_state = try source.currentSplitState(self.alloc, self.source_group_id);
        defer if (source_state) |state| shard_state_store.freeSplitState(self.alloc, state);
        if (source_state) |state| try self.validateActiveState(state);
        self.dest.deinit();
        self.dest_open = false;
        self.dest = try Destination.init(reopen_alloc, self.dest_cfg);
        self.dest_open = true;
    }

    pub fn validateTransitionCoordinates(self: *const SyncCoordinator, transition_id: u64, attempt_epoch: u64, source_group_id: u64, destination_group_id: u64) !void {
        if (self.transition_id != transition_id or self.attempt_epoch != attempt_epoch or
            self.source_group_id != source_group_id or self.dest_group_id != destination_group_id)
        {
            return error.ConflictingSplitTransition;
        }
    }

    fn validateActiveState(self: *const SyncCoordinator, state: shard_state_store.AppliedSplitState) !void {
        try validateActiveSplitDestination(state, self.transition_id, self.attempt_epoch, self.dest_group_id);
    }

    fn sourceStore(self: *SyncCoordinator) !*data_store.RaftApplyStore {
        if (!self.source_open) return error.SplitCoordinatorSourceClosed;
        return self.source;
    }

    fn destination(self: *SyncCoordinator) !*Destination {
        if (!self.dest_open) return error.SplitCoordinatorDestinationClosed;
        return &self.dest;
    }

    pub fn ensureBootstrapped(self: *SyncCoordinator) !bool {
        const source = try self.sourceStore();
        const dest = try self.destination();
        const source_state = try source.currentSplitState(self.alloc, self.source_group_id);
        defer if (source_state) |state| shard_state_store.freeSplitState(self.alloc, state);
        if (source_state) |state| try self.validateActiveState(state);
        const source_phase = if (source_state) |state| state.phase else null;
        if (source_phase == null) return false;
        if (source_phase.? != .splitting and source_phase.? != .finalizing) return false;

        if (try dest.db.getSplitBootstrapMarker(self.alloc)) |marker| {
            const same_attempt = marker.transition_id == self.transition_id and
                marker.attempt_epoch == self.attempt_epoch and
                marker.source_group_id == self.source_group_id and
                marker.destination_group_id == self.dest_group_id;
            if (same_attempt and marker.bootstrap_complete) return false;
        }

        const handoff = try source.captureSplitHandoff(self.alloc, self.source_group_id);
        defer shard_state_store.freeHandoff(self.alloc, handoff);
        return try dest.applyHandoff(self.alloc, handoff, .{
            .transition_id = self.transition_id,
            .attempt_epoch = self.attempt_epoch,
            .source_group_id = self.source_group_id,
            .destination_group_id = self.dest_group_id,
            .bootstrap_complete = true,
        });
    }

    pub fn startSourceSplit(self: *SyncCoordinator) !bool {
        const source = try self.sourceStore();
        const source_state = try source.currentSplitState(self.alloc, self.source_group_id);
        defer if (source_state) |state| shard_state_store.freeSplitState(self.alloc, state);
        const state = source_state orelse return false;
        try self.validateActiveState(state);
        if (state.phase != .prepare) return false;

        const op = try std.fmt.allocPrint(self.alloc, "split_start:{d}:{d}:{d}:{s}", .{
            self.transition_id,
            self.attempt_epoch,
            self.dest_group_id,
            state.split_key,
        });
        defer self.alloc.free(op);
        try self.applySourceControlEntry(op);
        return true;
    }

    pub fn prepareSourceSplit(self: *SyncCoordinator, split_key: []const u8, source_range_end: ?[]const u8) !bool {
        const source = try self.sourceStore();
        const source_state = try source.currentSplitState(self.alloc, self.source_group_id);
        defer if (source_state) |state| shard_state_store.freeSplitState(self.alloc, state);
        if (source_state) |state| {
            if (state.phase != .none) {
                try self.validateActiveState(state);
                if (!std.mem.eql(u8, state.split_key, split_key)) return error.ConflictingSplitTransition;
                return false;
            }

            const current_range = try source.currentRange(self.alloc, self.source_group_id);
            defer range_state.freeRange(self.alloc, current_range);
            const restore = try std.fmt.allocPrint(self.alloc, "range:{s}:{s}", .{
                current_range.start,
                state.original_range_end,
            });
            defer self.alloc.free(restore);
            try self.applySourceControlEntryVia(source, restore);
        } else if (source_range_end) |range_end| {
            const current_range = try source.currentRange(self.alloc, self.source_group_id);
            defer range_state.freeRange(self.alloc, current_range);
            const restore = try std.fmt.allocPrint(self.alloc, "range:{s}:{s}", .{
                current_range.start,
                range_end,
            });
            defer self.alloc.free(restore);
            try self.applySourceControlEntryVia(source, restore);
        }

        const op = try std.fmt.allocPrint(self.alloc, "split_prepare:{d}:{d}:{d}:{s}", .{ self.transition_id, self.attempt_epoch, self.dest_group_id, split_key });
        defer self.alloc.free(op);
        try self.applySourceControlEntryVia(source, op);
        return true;
    }

    pub fn catchUp(self: *SyncCoordinator) !usize {
        const source = try self.sourceStore();
        const dest = try self.destination();
        const source_state = try source.currentSplitState(self.alloc, self.source_group_id);
        defer if (source_state) |state| shard_state_store.freeSplitState(self.alloc, state);
        if (source_state) |state| try self.validateActiveState(state);
        const source_phase = if (source_state) |state| state.phase else null;
        if (source_phase == null) return 0;
        if (source_phase.? != .splitting and source_phase.? != .finalizing) return 0;

        const dest_range = dest.getRange();
        if (dest_range.start.len == 0 and dest_range.end.len == 0) return 0;

        const after_seq = try dest.appliedDeltaSequence(self.alloc);
        const deltas = try source.listSplitDeltasAfter(self.alloc, self.source_group_id, after_seq);
        defer shard_mod.freeDeltas(self.alloc, deltas);
        if (deltas.len == 0) return 0;
        try dest.applyDeltas(self.alloc, deltas);
        return deltas.len;
    }

    pub fn status(self: *SyncCoordinator) !SplitSyncStatus {
        const source = try self.sourceStore();
        const dest = try self.destination();
        const source_state = try source.currentSplitState(self.alloc, self.source_group_id);
        defer if (source_state) |state| shard_state_store.freeSplitState(self.alloc, state);
        if (source_state) |state| try self.validateActiveState(state);
        const source_terminal = try source.currentSplitTerminal(self.alloc, self.source_group_id);
        defer if (source_terminal) |terminal| shard_state_store.freeSplitTerminal(self.alloc, terminal);
        if (source_state == null) {
            if (source_terminal) |terminal| {
                if (self.attempt_epoch < terminal.attempt_epoch) return error.StaleSplitAttempt;
                if (self.attempt_epoch == terminal.attempt_epoch) {
                    try shard_state_store.validateSplitTerminalIdentity(
                        terminal,
                        self.transition_id,
                        self.attempt_epoch,
                        self.dest_group_id,
                        null,
                    );
                    return range_transition.completedSplitStatus(
                        terminal.outcome == .finalized,
                        try source.currentSplitDeltaSequence(self.alloc, self.source_group_id),
                        try dest.appliedDeltaSequence(self.alloc),
                    );
                }
            }
        }

        const bootstrap_marker = try dest.db.getSplitBootstrapMarker(self.alloc);
        const bootstrapped = if (bootstrap_marker) |marker|
            marker.transition_id == self.transition_id and
                marker.attempt_epoch == self.attempt_epoch and
                marker.source_group_id == self.source_group_id and
                marker.destination_group_id == self.dest_group_id and
                marker.bootstrap_complete
        else
            false;
        const source_phase = if (source_state) |state| state.phase else null;
        const source_seq = try source.currentSplitDeltaSequence(self.alloc, self.source_group_id);
        const dest_seq = try dest.appliedDeltaSequence(self.alloc);
        const derived = range_transition.deriveSplitStatus(source_phase, bootstrapped, source_seq, dest_seq);
        return .{
            .phase = derived.phase,
            .source_split_phase = derived.source_split_phase orelse switch (derived.phase) {
                .finalized, .rolled_back => .none,
                else => null,
            },
            .bootstrapped = derived.bootstrapped,
            .replay_required = derived.replay_required,
            .replay_caught_up = derived.replay_caught_up,
            .cutover_ready = derived.cutover_ready,
            .destination_ready_for_reads = derived.destination_ready_for_reads,
            .source_delta_sequence = derived.source_delta_sequence,
            .dest_delta_sequence = derived.dest_delta_sequence,
        };
    }

    pub fn finalizeSource(self: *SyncCoordinator) !bool {
        const transition_status = try self.status();
        if (transition_status.phase != .cutover_ready) return false;
        const source = try self.sourceStore();
        const source_state = (try source.currentSplitState(self.alloc, self.source_group_id)) orelse return false;
        defer shard_state_store.freeSplitState(self.alloc, source_state);
        try self.validateActiveState(source_state);
        const op = try std.fmt.allocPrint(self.alloc, "finalize_split:{d}:{d}:{d}:{s}", .{ self.transition_id, self.attempt_epoch, self.dest_group_id, source_state.split_key });
        defer self.alloc.free(op);
        try self.applySourceControlEntry(op);
        return true;
    }

    pub fn rollbackSource(self: *SyncCoordinator) !bool {
        const transition_status = try self.status();
        switch (transition_status.phase) {
            .prepare, .bootstrap_peer, .replay_deltas, .rolling_back => {},
            else => return false,
        }
        const source = try self.sourceStore();
        const source_state = (try source.currentSplitState(self.alloc, self.source_group_id)) orelse return false;
        defer shard_state_store.freeSplitState(self.alloc, source_state);
        try self.validateActiveState(source_state);
        const op = try std.fmt.allocPrint(self.alloc, "rollback_split:{d}:{d}:{d}:{s}", .{ self.transition_id, self.attempt_epoch, self.dest_group_id, source_state.split_key });
        defer self.alloc.free(op);
        try self.applySourceControlEntry(op);
        return true;
    }

    pub fn syncOnce(self: *SyncCoordinator) !SyncResult {
        const bootstrapped = try self.ensureBootstrapped();
        const applied_deltas = try self.catchUp();
        const transition_status = try self.status();
        return .{
            .bootstrapped = bootstrapped,
            .applied_deltas = applied_deltas,
            .replay_required = transition_status.replay_required,
            .replay_caught_up = transition_status.replay_caught_up,
            .cutover_ready = transition_status.cutover_ready,
        };
    }

    fn applySourceControlEntry(self: *SyncCoordinator, op: []const u8) !void {
        try self.applySourceControlEntryVia(try self.sourceStore(), op);
    }

    fn applySourceControlEntryVia(self: *SyncCoordinator, source: *data_store.RaftApplyStore, op: []const u8) !void {
        const latest = (try source.latestBatch(self.source_group_id)) orelse return error.MissingSplitSourceBatch;
        const term = if (latest.last_entry_term > 0) latest.last_entry_term else 1;
        const index = latest.last_entry_index + 1;
        const entries = try raft_state_machine.encodeCommittedEntries(self.alloc, &.{
            .{
                .term = term,
                .index = index,
                .entry_type = .normal,
                .data = @constCast(op),
            },
        });
        defer self.alloc.free(entries);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = self.source_group_id,
            .commit_index = index,
            .entries_bytes = entries,
        });
    }
};

pub const MergeCoordinator = struct {
    alloc: std.mem.Allocator,
    donor_root_dir: []u8,
    receiver_root_dir: []u8,
    donor_group_id: u64,
    receiver_group_id: u64,
    donor_cfg: data_store.RaftApplyStoreConfig,
    receiver_cfg: DestinationConfig,
    donor: *data_store.RaftApplyStore,
    donor_owned: bool,
    receiver: Destination,
    receiver_accepts_donor_range: bool,
    receiver_base_range: db_types.ByteRange,
    merge_phase: MergeLifecyclePhase,
    allow_doc_identity_reassignment: bool,
    receiver_identity_reassignment_namespace: ?doc_identity.Namespace,

    pub fn init(alloc: std.mem.Allocator, cfg: MergeConfig) !MergeCoordinator {
        const donor_root_dir = try alloc.dupe(u8, cfg.donor_root_dir);
        var donor_root_dir_owned = true;
        errdefer if (donor_root_dir_owned) alloc.free(donor_root_dir);
        const receiver_root_dir = try alloc.dupe(u8, cfg.receiver_root_dir);
        errdefer alloc.free(receiver_root_dir);

        var donor_cfg = cfg.donor;
        donor_cfg.root_dir = donor_root_dir;
        donor_root_dir_owned = false;
        errdefer freeConfig(alloc, donor_cfg);

        var receiver_cfg = cfg.receiver;
        receiver_cfg.root_dir = receiver_root_dir;

        var donor_owned = false;
        const donor = cfg.donor_store orelse donor: {
            const owned = try alloc.create(data_store.RaftApplyStore);
            errdefer alloc.destroy(owned);
            owned.* = try data_store.RaftApplyStore.init(alloc, donor_cfg);
            donor_owned = true;
            break :donor owned;
        };
        errdefer if (donor_owned) {
            donor.deinit();
            alloc.destroy(donor);
        };
        var receiver = if (cfg.receiver_lease) |lease|
            try Destination.initBorrowed(alloc, receiver_root_dir, lease)
        else
            try Destination.init(alloc, receiver_cfg);
        errdefer receiver.deinit();

        const persisted = try receiver.loadMergeState(alloc);
        defer if (persisted) |state| range_state.freeRange(alloc, state.receiver_base_range);

        const base_range: db_types.ByteRange = if (persisted) |state| .{
            .start = try alloc.dupe(u8, state.receiver_base_range.start),
            .end = try alloc.dupe(u8, state.receiver_base_range.end),
        } else .{
            .start = try alloc.dupe(u8, receiver.getRange().start),
            .end = try alloc.dupe(u8, receiver.getRange().end),
        };
        errdefer range_state.freeRange(alloc, base_range);

        return .{
            .alloc = alloc,
            .donor_root_dir = donor_root_dir,
            .receiver_root_dir = receiver_root_dir,
            .donor_group_id = cfg.donor_group_id,
            .receiver_group_id = cfg.receiver_group_id,
            .donor_cfg = donor_cfg,
            .receiver_cfg = receiver_cfg,
            .donor = donor,
            .donor_owned = donor_owned,
            .receiver = receiver,
            .receiver_accepts_donor_range = if (persisted) |state|
                state.phase == .accepting or state.phase == .finalized or state.phase == .rolling_back
            else
                false,
            .receiver_base_range = base_range,
            .merge_phase = if (persisted) |state| state.phase else .none,
            .allow_doc_identity_reassignment = if (persisted) |state| state.allow_doc_identity_reassignment else false,
            .receiver_identity_reassignment_namespace = cfg.receiver_identity_reassignment_namespace orelse if (persisted) |state|
                state.receiver_identity_reassignment_namespace
            else
                null,
        };
    }

    pub fn deinit(self: *MergeCoordinator) void {
        self.receiver.deinit();
        if (self.donor_owned) {
            self.donor.deinit();
            self.alloc.destroy(self.donor);
        }
        range_state.freeRange(self.alloc, self.receiver_base_range);
        freeConfig(self.alloc, self.donor_cfg);
        self.alloc.free(self.receiver_root_dir);
        self.* = undefined;
    }

    pub fn reopenDonor(self: *MergeCoordinator) !void {
        if (!self.donor_owned) return error.BorrowedMergeDonorStore;
        self.donor.deinit();
        self.donor.* = try data_store.RaftApplyStore.init(self.alloc, self.donor_cfg);
    }

    pub fn reopenReceiver(self: *MergeCoordinator) !void {
        if (self.receiver.owned_db == null) return error.BorrowedMergeReceiverDb;
        self.receiver.deinit();
        self.receiver = try Destination.init(self.alloc, self.receiver_cfg);
    }

    pub fn acceptDonorRange(self: *MergeCoordinator) !void {
        try self.requireConfiguredReceiverIdentityReassignmentOptIn();
        self.receiver_accepts_donor_range = true;
        self.merge_phase = .accepting;
        try self.persistMergeState();
    }

    pub fn recordDocIdentityReassignmentOptIn(self: *MergeCoordinator) !void {
        const was_allowed = self.allow_doc_identity_reassignment;
        self.allow_doc_identity_reassignment = true;
        if (self.receiver_identity_reassignment_namespace) |namespace| {
            self.reassignReceiverIdentityNamespace(namespace) catch |err| {
                if (!was_allowed) self.allow_doc_identity_reassignment = false;
                return err;
            };
        }
        if (!was_allowed and self.merge_phase != .none) try self.persistMergeState();
    }

    pub fn reassignReceiverIdentityNamespace(self: *MergeCoordinator, namespace: doc_identity.Namespace) !void {
        if (!self.allow_doc_identity_reassignment) return error.DocIdentityReassignmentNotAllowed;
        try self.receiver.db.reassignIdentityNamespaceForInternalTransition(namespace);
    }

    pub fn ensureReceiverBootstrapped(self: *MergeCoordinator) !bool {
        try self.requireConfiguredReceiverIdentityReassignmentOptIn();
        if (!self.receiver_accepts_donor_range or self.merge_phase == .rolled_back) return false;
        const donor_range = try self.donor.currentRange(self.alloc, self.donor_group_id);
        defer range_state.freeRange(self.alloc, donor_range);

        if (receiverCoversDonor(self.receiver.getRange(), donor_range)) return false;

        const donor_entries = try self.donor.groupState(self.alloc, self.donor_group_id);
        defer shard_state_store.freeGroupStateEntries(self.alloc, donor_entries);
        const donor_applied_index = try self.donorAppliedIndex();
        try self.receiver.applyMergeBootstrap(self.alloc, .{
            .start = donor_range.start,
            .end = donor_range.end,
        }, donor_entries, donor_applied_index);
        return true;
    }

    pub fn catchUp(self: *MergeCoordinator) !usize {
        try self.requireConfiguredReceiverIdentityReassignmentOptIn();
        if (!self.receiver_accepts_donor_range or self.merge_phase != .accepting) return 0;

        const donor_range = try self.donor.currentRange(self.alloc, self.donor_group_id);
        defer range_state.freeRange(self.alloc, donor_range);
        if (!receiverCoversDonor(self.receiver.getRange(), donor_range)) return 0;

        const after_index = try self.receiver.appliedDeltaSequence(self.alloc);
        const donor_entries = try self.donor.appliedNormalEntries(self.alloc, self.donor_group_id);
        defer {
            for (donor_entries) |entry| self.alloc.free(@constCast(entry.data));
            self.alloc.free(donor_entries);
        }

        var replay_ops = std.ArrayListUnmanaged(ReplayOperation).empty;
        defer {
            for (replay_ops.items) |op| switch (op) {
                .put => |put| {
                    self.alloc.free(put.key);
                    self.alloc.free(put.value);
                },
                .delete => |key| self.alloc.free(key),
            };
            replay_ops.deinit(self.alloc);
        }

        var applied: usize = 0;
        var max_index = after_index;
        for (donor_entries) |entry| {
            if (entry.index <= after_index) continue;
            if (try parseReplayOperation(self.alloc, entry.data)) |op| {
                try replay_ops.append(self.alloc, op);
            }
            max_index = @max(max_index, entry.index);
            applied += 1;
        }

        if (max_index == after_index) return 0;
        try self.receiver.applyMergeReplay(self.alloc, .{
            .start = donor_range.start,
            .end = donor_range.end,
        }, replay_ops.items, max_index);
        return applied;
    }

    pub fn status(self: *MergeCoordinator) !range_transition.MergeStatus {
        const donor_range = try self.donor.currentRange(self.alloc, self.donor_group_id);
        defer range_state.freeRange(self.alloc, donor_range);
        const receiver_range = self.receiver.getRange();
        const bootstrapped = receiverCoversDonor(receiver_range, donor_range) and
            !rangesEqual(receiver_range, self.receiver_base_range);
        const donor_seq = try self.donorAppliedIndex();
        const receiver_seq = try self.receiver.appliedDeltaSequence(self.alloc);
        var merge_status = range_transition.deriveMergeStatus(
            self.donor_group_id,
            self.receiver_group_id,
            self.receiver_accepts_donor_range,
            bootstrapped,
            donor_seq,
            receiver_seq,
            self.merge_phase == .rolling_back,
            self.merge_phase == .finalized,
            self.merge_phase == .rolled_back,
        );
        merge_status.allow_doc_identity_reassignment = self.allow_doc_identity_reassignment;
        if (self.receiver_identity_reassignment_namespace) |namespace| {
            merge_status.receiver_identity_reassignment_namespace_table_id = namespace.table_id;
            merge_status.receiver_identity_reassignment_namespace_shard_id = namespace.shard_id;
            merge_status.receiver_identity_reassignment_namespace_range_id = namespace.range_id;
        }
        return merge_status;
    }

    pub fn syncOnce(self: *MergeCoordinator) !range_transition.MergeStatus {
        _ = try self.ensureReceiverBootstrapped();
        _ = try self.catchUp();
        return try self.status();
    }

    pub fn finalizeMerge(self: *MergeCoordinator) !bool {
        try self.requireConfiguredReceiverIdentityReassignmentOptIn();
        const transition_status = try self.status();
        if (transition_status.phase != .cutover_ready) return false;
        self.merge_phase = .finalized;
        try self.persistMergeState();
        return true;
    }

    pub fn rollbackMerge(self: *MergeCoordinator) !bool {
        if (self.allow_doc_identity_reassignment) try self.requireConfiguredReceiverIdentityReassignmentOptIn();
        const transition_status = try self.status();
        switch (transition_status.phase) {
            .prepare, .bootstrap_peer, .replay_deltas, .cutover_ready => {},
            else => return false,
        }

        self.merge_phase = .rolling_back;
        try self.persistMergeState();

        const donor_range = try self.donor.currentRange(self.alloc, self.donor_group_id);
        defer range_state.freeRange(self.alloc, donor_range);
        try self.receiver.deleteDocsInRange(self.alloc, .{
            .start = donor_range.start,
            .end = donor_range.end,
        });
        try self.receiver.db.updateRange(.{
            .start = self.receiver_base_range.start,
            .end = self.receiver_base_range.end,
        });
        try self.receiver.db.clearSplitDeltaFinalSeq();
        self.receiver_accepts_donor_range = false;
        self.merge_phase = .rolled_back;
        try self.persistMergeState();
        return true;
    }

    fn donorAppliedIndex(self: *MergeCoordinator) !u64 {
        const latest = (try self.donor.latestBatch(self.donor_group_id)) orelse return 0;
        return latest.last_entry_index;
    }

    fn persistMergeState(self: *MergeCoordinator) !void {
        try self.receiver.saveMergeState(self.alloc, .{
            .donor_group_id = self.donor_group_id,
            .receiver_group_id = self.receiver_group_id,
            .phase = self.merge_phase,
            .receiver_base_range = .{
                .start = self.receiver_base_range.start,
                .end = self.receiver_base_range.end,
            },
            .allow_doc_identity_reassignment = self.allow_doc_identity_reassignment,
            .receiver_identity_reassignment_namespace = self.receiver_identity_reassignment_namespace,
        });
    }

    fn requireConfiguredReceiverIdentityReassignmentOptIn(self: *MergeCoordinator) !void {
        if (self.receiver_identity_reassignment_namespace) |namespace| {
            if (!self.allow_doc_identity_reassignment) return error.DocIdentityReassignmentNotAllowed;
            try self.reassignReceiverIdentityNamespace(namespace);
        }
    }
};

pub const SyncResult = struct {
    bootstrapped: bool,
    applied_deltas: usize,
    replay_required: bool,
    replay_caught_up: bool,
    cutover_ready: bool,
};

pub const SplitSyncStatus = range_transition.SplitStatus;

pub const MergeSyncStatus = range_transition.MergeStatus;

const ReplayOperation = union(enum) {
    put: struct {
        key: []u8,
        value: []u8,
    },
    delete: []u8,
};

fn freeConfig(alloc: std.mem.Allocator, cfg: data_store.RaftApplyStoreConfig) void {
    if (cfg.root_dir.len > 0) alloc.free(@constCast(cfg.root_dir));
}

fn decodeDocumentKeyAlloc(alloc: std.mem.Allocator, internal_key: []const u8) ![]u8 {
    const logical_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, internal_key)) orelse return error.InvalidAppliedDataDocumentKey;
    defer alloc.free(logical_key);
    if (!std.mem.startsWith(u8, logical_key, "g:")) return error.InvalidAppliedDataDocumentKey;
    const sep = std.mem.indexOfScalarPos(u8, logical_key, 2, ':') orelse return error.InvalidAppliedDataDocumentKey;
    return try alloc.dupe(u8, logical_key[sep + 1 ..]);
}

fn mergeRanges(left: db_types.ByteRange, right: db_types.ByteRange) db_types.ByteRange {
    return .{
        .start = if (left.start.len == 0 or (right.start.len > 0 and std.mem.order(u8, right.start, left.start) == .lt))
            right.start
        else
            left.start,
        .end = if (left.end.len == 0 or right.end.len == 0)
            if (left.end.len == 0) left.end else right.end
        else if (std.mem.order(u8, right.end, left.end) == .gt)
            right.end
        else
            left.end,
    };
}

fn rangesEqual(left: db_types.ByteRange, right: db_types.ByteRange) bool {
    return std.mem.eql(u8, left.start, right.start) and std.mem.eql(u8, left.end, right.end);
}

fn receiverCoversDonor(receiver: db_types.ByteRange, donor: db_types.ByteRange) bool {
    const starts_ok = receiver.start.len == 0 or donor.start.len == 0 or std.mem.order(u8, receiver.start, donor.start) != .gt;
    const ends_ok = receiver.end.len == 0 or donor.end.len == 0 or std.mem.order(u8, receiver.end, donor.end) != .lt;
    return starts_ok and ends_ok;
}

fn encodeMergeState(list: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, state: PersistedMergeState) !void {
    try list.append(alloc, @intFromEnum(state.phase));
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, state.donor_group_id)));
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, state.receiver_group_id)));
    const start_len: u32 = @intCast(state.receiver_base_range.start.len);
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u32, start_len)));
    try list.appendSlice(alloc, state.receiver_base_range.start);
    const end_len: u32 = @intCast(state.receiver_base_range.end.len);
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u32, end_len)));
    try list.appendSlice(alloc, state.receiver_base_range.end);
    try list.append(alloc, if (state.allow_doc_identity_reassignment) 1 else 0);
    if (state.receiver_identity_reassignment_namespace) |namespace| {
        try list.append(alloc, 1);
        try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, namespace.table_id)));
        try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, namespace.shard_id)));
        try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, namespace.range_id)));
    } else {
        try list.append(alloc, 0);
    }
}

fn decodeMergeStateAlloc(alloc: std.mem.Allocator, data: []const u8) !PersistedMergeState {
    if (data.len < 1 + 8 + 8 + 4 + 4) return error.InvalidMergeState;
    var pos: usize = 0;
    const phase: MergeLifecyclePhase = @enumFromInt(data[pos]);
    pos += 1;
    const donor_group_id = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const receiver_group_id = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const start_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (pos + start_len > data.len) return error.InvalidMergeState;
    const start = try alloc.dupe(u8, data[pos .. pos + start_len]);
    errdefer alloc.free(start);
    pos += start_len;
    const end_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (pos + end_len > data.len) return error.InvalidMergeState;
    const end = try alloc.dupe(u8, data[pos .. pos + end_len]);
    pos += end_len;
    const allow_doc_identity_reassignment = if (pos < data.len) blk: {
        const allowed = data[pos] != 0;
        pos += 1;
        break :blk allowed;
    } else false;
    const receiver_identity_reassignment_namespace: ?doc_identity.Namespace = if (pos < data.len) blk: {
        const has_namespace = data[pos] != 0;
        pos += 1;
        if (!has_namespace) break :blk null;
        if (pos + 24 > data.len) return error.InvalidMergeState;
        const table_id = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        const shard_id = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        const range_id = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        break :blk .{ .table_id = table_id, .shard_id = shard_id, .range_id = range_id };
    } else null;
    return .{
        .donor_group_id = donor_group_id,
        .receiver_group_id = receiver_group_id,
        .phase = phase,
        .receiver_base_range = .{
            .start = start,
            .end = end,
        },
        .allow_doc_identity_reassignment = allow_doc_identity_reassignment,
        .receiver_identity_reassignment_namespace = receiver_identity_reassignment_namespace,
    };
}

fn parseReplayOperation(alloc: std.mem.Allocator, data: []const u8) !?ReplayOperation {
    if (std.mem.startsWith(u8, data, "put:")) {
        const rest = data["put:".len..];
        const eq = std.mem.indexOfScalar(u8, rest, '=') orelse return error.InvalidAppliedDataOperation;
        return .{
            .put = .{
                .key = try alloc.dupe(u8, rest[0..eq]),
                .value = try alloc.dupe(u8, rest[eq + 1 ..]),
            },
        };
    }
    if (std.mem.startsWith(u8, data, "del:")) {
        return .{ .delete = try alloc.dupe(u8, data["del:".len..]) };
    }
    return null;
}

test "db split destination read-only open does not create missing root" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-handoff-readonly-missing", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), dst_root, .{}));

    try std.testing.expectError(error.FileNotFound, Destination.initReadOnly(std.testing.allocator, dst_root));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), dst_root, .{}));
}

test "db split destination applies handoff and filtered split deltas" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-handoff-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-handoff-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var src = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer src.deinit();
    var dst = try Destination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dst.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:90:1:90:doc:m") },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("split_start:90:1:90:doc:m") },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = @constCast("put:doc:u={\"v\":\"right-1\"}") },
    });
    defer std.testing.allocator.free(setup);
    try src.snapshotBuilder().applyBatch(.{
        .group_id = 101,
        .commit_index = 6,
        .entries_bytes = setup,
    });

    const handoff = try src.captureSplitHandoff(std.testing.allocator, 101);
    defer shard_state_store.freeHandoff(std.testing.allocator, handoff);
    _ = try dst.applyHandoff(std.testing.allocator, handoff, .{
        .transition_id = 90,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 90,
        .bootstrap_complete = true,
    });
    try std.testing.expectEqual(@as(u64, 1), try dst.appliedDeltaSequence(std.testing.allocator));
    try std.testing.expectEqualStrings("doc:m", dst.getRange().start);
    try std.testing.expectEqualStrings("doc:z", dst.getRange().end);

    const catchup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = @constCast("put:doc:c={\"v\":\"left-1\"}") },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = @constCast("put:doc:x={\"v\":\"right-2\"}") },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = @constCast("del:doc:t") },
    });
    defer std.testing.allocator.free(catchup);
    try src.snapshotBuilder().applyBatch(.{
        .group_id = 101,
        .commit_index = 9,
        .entries_bytes = catchup,
    });

    const deltas = try src.listSplitDeltasAfter(std.testing.allocator, 101, handoff.base_delta_sequence);
    defer shard_mod.freeDeltas(std.testing.allocator, deltas);
    try dst.applyDeltas(std.testing.allocator, deltas);

    try std.testing.expectEqual(@as(u64, 2), try dst.appliedDeltaSequence(std.testing.allocator));
    try std.testing.expect((try dst.get(std.testing.allocator, "doc:t")) == null);
    const right = (try dst.get(std.testing.allocator, "doc:x")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(right);
    try std.testing.expectEqualStrings("{\"v\":\"right-2\"}", right);
    try std.testing.expect((try dst.get(std.testing.allocator, "doc:c")) == null);
}

test "db split successor bootstrap atomically replaces stale destination generation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-handoff-successor", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);
    var dst = try Destination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dst.deinit();

    var first_entries = [_]shard_state_store.AppliedDataKV{
        .{ .key = "doc:m", .value = "{\"v\":1}" },
        .{ .key = "doc:t", .value = "{\"stale\":true}" },
    };
    const first = data_store.SplitHandoff{
        .byte_range = .{ .start = "doc:m", .end = "doc:z" },
        .split_state = .{
            .phase = .splitting,
            .transition_id = 80,
            .attempt_epoch = 1,
            .split_key = "doc:m",
            .new_shard_id = 200,
            .original_range_end = "doc:z",
        },
        .base_delta_sequence = 7,
        .entries = &first_entries,
    };
    try std.testing.expect(try dst.applyHandoff(std.testing.allocator, first, .{
        .transition_id = 80,
        .attempt_epoch = 1,
        .source_group_id = 100,
        .destination_group_id = 200,
        .bootstrap_complete = true,
    }));

    var successor_entries = [_]shard_state_store.AppliedDataKV{
        .{ .key = "doc:n", .value = "{\"v\":2}" },
    };
    const successor = data_store.SplitHandoff{
        .byte_range = .{ .start = "doc:m", .end = "doc:z" },
        .split_state = .{
            .phase = .splitting,
            .transition_id = 81,
            .attempt_epoch = 2,
            .split_key = "doc:m",
            .new_shard_id = 200,
            .original_range_end = "doc:z",
        },
        .base_delta_sequence = 3,
        .entries = &successor_entries,
    };
    try std.testing.expect(try dst.applyHandoff(std.testing.allocator, successor, .{
        .transition_id = 81,
        .attempt_epoch = 2,
        .source_group_id = 100,
        .destination_group_id = 200,
        .bootstrap_complete = true,
    }));

    try std.testing.expect((try dst.get(std.testing.allocator, "doc:m")) == null);
    try std.testing.expect((try dst.get(std.testing.allocator, "doc:t")) == null);
    const current = (try dst.get(std.testing.allocator, "doc:n")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(current);
    try std.testing.expectEqualStrings("{\"v\":2}", current);
    try std.testing.expectEqual(@as(u64, 3), try dst.appliedDeltaSequence(std.testing.allocator));
    const marker = (try dst.db.getSplitBootstrapMarker(std.testing.allocator)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 81), marker.transition_id);
    try std.testing.expectEqual(@as(u64, 2), marker.attempt_epoch);
}

test "db split destination persists handoff state across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-handoff-reopen-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-handoff-reopen-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var src = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer src.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_prepare:91:1:91:doc:m") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_start:91:1:91:doc:m") },
    });
    defer std.testing.allocator.free(setup);
    try src.snapshotBuilder().applyBatch(.{
        .group_id = 111,
        .commit_index = 4,
        .entries_bytes = setup,
    });

    const handoff = try src.captureSplitHandoff(std.testing.allocator, 111);
    defer shard_state_store.freeHandoff(std.testing.allocator, handoff);

    {
        var dst = try Destination.init(std.testing.allocator, .{ .root_dir = dst_root });
        defer dst.deinit();
        _ = try dst.applyHandoff(std.testing.allocator, handoff, .{
            .transition_id = 91,
            .attempt_epoch = 1,
            .source_group_id = 111,
            .destination_group_id = 91,
            .bootstrap_complete = true,
        });
        try std.testing.expectEqual(@as(u64, 0), try dst.appliedDeltaSequence(std.testing.allocator));

        const catchup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("put:doc:x={\"v\":\"right-1\"}") },
        });
        defer std.testing.allocator.free(catchup);
        try src.snapshotBuilder().applyBatch(.{
            .group_id = 111,
            .commit_index = 5,
            .entries_bytes = catchup,
        });

        const deltas = try src.listSplitDeltasAfter(std.testing.allocator, 111, handoff.base_delta_sequence);
        defer shard_mod.freeDeltas(std.testing.allocator, deltas);
        try dst.applyDeltas(std.testing.allocator, deltas);
        try std.testing.expectEqual(@as(u64, 1), try dst.appliedDeltaSequence(std.testing.allocator));
    }

    {
        var reopened = try Destination.init(std.testing.allocator, .{ .root_dir = dst_root });
        defer reopened.deinit();
        try std.testing.expectEqualStrings("doc:m", reopened.getRange().start);
        try std.testing.expectEqualStrings("doc:z", reopened.getRange().end);
        try std.testing.expectEqual(@as(u64, 1), try reopened.appliedDeltaSequence(std.testing.allocator));
        const right = (try reopened.get(std.testing.allocator, "doc:x")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(right);
        try std.testing.expectEqualStrings("{\"v\":\"right-1\"}", right);
    }
}

test "db split sync coordinator resumes catch-up across source and destination reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();
        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:92:1:122:doc:m") },
            .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("split_start:92:1:122:doc:m") },
        });
        defer std.testing.allocator.free(setup);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 121,
            .commit_index = 5,
            .entries_bytes = setup,
        });
    }

    var coord = try SyncCoordinator.init(std.testing.allocator, .{
        .transition_id = 92,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 121,
        .dest_group_id = 122,
    });
    defer coord.deinit();

    // Placement provisions the destination range before data handoff. Range
    // ownership alone must never be interpreted as completed bootstrap.
    try coord.dest.db.updateRange(.{ .start = "doc:m", .end = "doc:z" });

    {
        const result = try coord.syncOnce();
        try std.testing.expect(result.bootstrapped);
        try std.testing.expectEqual(@as(usize, 0), result.applied_deltas);
        try std.testing.expect(result.replay_required);
        try std.testing.expect(result.replay_caught_up);
        try std.testing.expect(result.cutover_ready);
        try std.testing.expectEqual(@as(u64, 0), try coord.dest.appliedDeltaSequence(std.testing.allocator));
    }

    const catchup_1 = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = @constCast("put:doc:u={\"v\":\"right-1\"}") },
    });
    defer std.testing.allocator.free(catchup_1);
    try coord.source.snapshotBuilder().applyBatch(.{
        .group_id = 121,
        .commit_index = 6,
        .entries_bytes = catchup_1,
    });

    {
        const result = try coord.syncOnce();
        try std.testing.expect(!result.bootstrapped);
        try std.testing.expectEqual(@as(usize, 1), result.applied_deltas);
        try std.testing.expect(result.replay_required);
        try std.testing.expect(result.replay_caught_up);
        try std.testing.expect(result.cutover_ready);
        try std.testing.expectEqual(@as(u64, 1), try coord.dest.appliedDeltaSequence(std.testing.allocator));
    }

    try coord.reopenSource();
    try coord.reopenDestination();

    {
        const status = try coord.status();
        try std.testing.expect(status.bootstrapped);
        try std.testing.expect(status.replay_required);
        try std.testing.expect(status.replay_caught_up);
        try std.testing.expect(status.cutover_ready);
        try std.testing.expectEqual(@as(u64, 1), status.source_delta_sequence);
        try std.testing.expectEqual(@as(u64, 1), status.dest_delta_sequence);
    }

    const catchup_2 = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = @constCast("put:doc:x={\"v\":\"right-2\"}") },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = @constCast("put:doc:c={\"v\":\"left-1\"}") },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = @constCast("del:doc:t") },
    });
    defer std.testing.allocator.free(catchup_2);
    try coord.source.snapshotBuilder().applyBatch(.{
        .group_id = 121,
        .commit_index = 9,
        .entries_bytes = catchup_2,
    });

    {
        const result = try coord.syncOnce();
        try std.testing.expect(!result.bootstrapped);
        try std.testing.expectEqual(@as(usize, 1), result.applied_deltas);
        try std.testing.expect(result.replay_required);
        try std.testing.expect(result.replay_caught_up);
        try std.testing.expect(result.cutover_ready);
        try std.testing.expectEqual(@as(u64, 2), try coord.dest.appliedDeltaSequence(std.testing.allocator));
        try std.testing.expect((try coord.dest.get(std.testing.allocator, "doc:t")) == null);
        const u = (try coord.dest.get(std.testing.allocator, "doc:u")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(u);
        try std.testing.expectEqualStrings("{\"v\":\"right-1\"}", u);
        const x = (try coord.dest.get(std.testing.allocator, "doc:x")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(x);
        try std.testing.expectEqualStrings("{\"v\":\"right-2\"}", x);
        try std.testing.expect((try coord.dest.get(std.testing.allocator, "doc:c")) == null);
    }
}

test "db split sync coordinator allocates destination identity namespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-identity-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-identity-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();
        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_prepare:7022:1:7022:doc:m") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_start:7022:1:7022:doc:m") },
        });
        defer std.testing.allocator.free(setup);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 7021,
            .commit_index = 4,
            .entries_bytes = setup,
        });
    }

    const destination_namespace = doc_identity.Namespace{
        .table_id = 70,
        .shard_id = 7022,
        .range_id = 9102,
    };
    var coord = try SyncCoordinator.init(std.testing.allocator, .{
        .transition_id = 7022,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 7021,
        .dest_group_id = 7022,
        .dest = .{ .root_dir = dst_root, .db = .{ .identity_namespace = destination_namespace } },
    });
    defer coord.deinit();

    const result = try coord.syncOnce();
    try std.testing.expect(result.bootstrapped);
    const value = (try coord.dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(value);
    try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", value);

    const stats = try coord.dest.db.stats(std.testing.allocator);
    try std.testing.expectEqual(destination_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(destination_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(destination_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.allocated_ordinals);
    try std.testing.expect(!stats.doc_identity.rebuild_required);
}

test "db split status rejects stale destination identity namespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-status-stale-identity-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-status-stale-identity-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();
    }

    const stale_namespace = doc_identity.Namespace{
        .table_id = 70,
        .shard_id = 7022,
        .range_id = 9101,
    };
    const expected_namespace = doc_identity.Namespace{
        .table_id = 70,
        .shard_id = 7022,
        .range_id = 9102,
    };
    {
        var dest = try Destination.init(std.testing.allocator, .{
            .root_dir = dst_root,
            .db = .{ .identity_namespace = stale_namespace },
        });
        defer dest.deinit();
        try dest.db.batch(.{
            .writes = &.{.{ .key = "doc:t", .value = "{\"v\":\"right-0\"}" }},
        });
    }

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, observeSplitStatus(std.testing.allocator, .{
        .transition_id = 7022,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 7021,
        .dest_group_id = 7022,
        .source = .{ .root_dir = src_root },
        .dest = .{
            .root_dir = dst_root,
            .db = .{
                .identity_namespace = expected_namespace,
                .prefer_existing_identity_namespace = true,
            },
        },
    }));
}

test "db split status borrows the live raft apply store without a second writer" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-status-borrowed-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-status-borrowed-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer source.deinit();
    {
        var dest = try Destination.init(std.testing.allocator, .{ .root_dir = dst_root });
        dest.deinit();
    }

    const status = try observeSplitStatus(std.testing.allocator, .{
        .transition_id = 7002,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 7001,
        .dest_group_id = 7002,
        .source = .{ .root_dir = src_root },
        .source_store = &source,
        .dest = .{ .root_dir = dst_root },
    });
    try std.testing.expectEqual(SplitTransitionPhase.prepare, status.phase);
    try std.testing.expect(!status.bootstrapped);
}

test "db split status uses source acknowledgement without opening destination" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-status-source-ack-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const progress_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-status-source-ack-progress", .{tmp.sub_path});
    defer std.testing.allocator.free(progress_root);
    const missing_dest_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-status-source-ack-missing-dest", .{tmp.sub_path});
    defer std.testing.allocator.free(missing_dest_root);

    var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer source.deinit();
    var progress = try db_mod.DB.open(std.testing.allocator, progress_root, .{});
    defer progress.close();
    try progress.setSplitBootstrapMarker(.{
        .transition_id = 7002,
        .attempt_epoch = 1,
        .source_group_id = 7001,
        .destination_group_id = 7002,
        .bootstrap_complete = true,
    });
    try progress.setSplitDeltaFinalSeq(4);

    const status = try observeSplitStatus(std.testing.allocator, .{
        .transition_id = 7002,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = missing_dest_root,
        .source_group_id = 7001,
        .dest_group_id = 7002,
        .source = .{ .root_dir = src_root },
        .source_store = &source,
        .progress_db = &progress,
    });
    try std.testing.expectEqual(SplitTransitionPhase.prepare, status.phase);
    try std.testing.expect(!status.bootstrapped);
    try std.testing.expect(!status.destination_ready_for_reads);
}

test "db split sync coordinator tracks explicit split transition phases" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-phase-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-phase-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer source.deinit();

    const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:132:1:132:doc:m") },
    });
    defer std.testing.allocator.free(prepare);
    try source.snapshotBuilder().applyBatch(.{
        .group_id = 131,
        .commit_index = 4,
        .entries_bytes = prepare,
    });

    var coord = try SyncCoordinator.init(std.testing.allocator, .{
        .transition_id = 132,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 131,
        .dest_group_id = 132,
        .source_store = &source,
    });
    defer coord.deinit();

    {
        const status = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.prepare, status.phase);
        try std.testing.expectEqual(shard_mod.SplitPhase.prepare, status.source_split_phase.?);
        try std.testing.expect(!status.bootstrapped);
        try std.testing.expect(!status.destination_ready_for_reads);
        const result = try coord.syncOnce();
        try std.testing.expect(!result.bootstrapped);
        try std.testing.expectEqual(@as(usize, 0), result.applied_deltas);
    }

    const start = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("split_start:132:1:132:doc:m") },
    });
    defer std.testing.allocator.free(start);
    try coord.source.snapshotBuilder().applyBatch(.{
        .group_id = 131,
        .commit_index = 5,
        .entries_bytes = start,
    });

    {
        const status = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.bootstrap_peer, status.phase);
        const result = try coord.syncOnce();
        try std.testing.expect(result.bootstrapped);
        try std.testing.expectEqual(@as(usize, 0), result.applied_deltas);
        const post = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.cutover_ready, post.phase);
        try std.testing.expect(post.destination_ready_for_reads);
    }

    const delta = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = @constCast("put:doc:x={\"v\":\"right-1\"}") },
    });
    defer std.testing.allocator.free(delta);
    try coord.source.snapshotBuilder().applyBatch(.{
        .group_id = 131,
        .commit_index = 6,
        .entries_bytes = delta,
    });

    {
        const status = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.replay_deltas, status.phase);
        try std.testing.expect(!status.destination_ready_for_reads);
        const result = try coord.syncOnce();
        try std.testing.expect(!result.bootstrapped);
        try std.testing.expectEqual(@as(usize, 1), result.applied_deltas);
        const post = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.cutover_ready, post.phase);
        try std.testing.expect(post.destination_ready_for_reads);
    }

    try std.testing.expect(try coord.finalizeSource());
    {
        const status = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.finalized, status.phase);
        try std.testing.expect(status.destination_ready_for_reads);
    }
}

test "db split sync coordinator can start source split from prepare" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-start-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-start-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer source.deinit();

    const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("split_prepare:134:1:134:doc:m") },
    });
    defer std.testing.allocator.free(prepare);
    try source.snapshotBuilder().applyBatch(.{
        .group_id = 133,
        .commit_index = 2,
        .entries_bytes = prepare,
    });

    var coord = try SyncCoordinator.init(std.testing.allocator, .{
        .transition_id = 134,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 133,
        .dest_group_id = 134,
        .source_store = &source,
    });
    defer coord.deinit();

    const before = try coord.status();
    try std.testing.expectEqual(SplitTransitionPhase.prepare, before.phase);
    try std.testing.expect(try coord.startSourceSplit());
    const after = try coord.status();
    try std.testing.expectEqual(SplitTransitionPhase.bootstrap_peer, after.phase);
}

test "db split sync coordinator can prepare source split again after rollback" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-reprepare-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-reprepare-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:136:1:136:doc:m") },
        });
        defer std.testing.allocator.free(setup);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 135,
            .commit_index = 4,
            .entries_bytes = setup,
        });
    }

    var coord = try SyncCoordinator.init(std.testing.allocator, .{
        .transition_id = 136,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 135,
        .dest_group_id = 136,
    });
    defer coord.deinit();

    try std.testing.expect(try coord.startSourceSplit());
    {
        const started = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.bootstrap_peer, started.phase);
    }

    try std.testing.expect(try coord.rollbackSource());
    {
        const rolled_back = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.rolled_back, rolled_back.phase);
        try std.testing.expectEqual(shard_state_store.SplitPhase.none, rolled_back.source_split_phase);
    }

    // A durable rollback terminal fences the completed attempt. Re-prepare as
    // a successor attempt instead of reusing the rolled-back identity.
    coord.transition_id = 137;
    coord.attempt_epoch = 2;
    try std.testing.expect(try coord.prepareSourceSplit("doc:m", "doc:z"));
    {
        const prepared = try coord.status();
        try std.testing.expectEqual(SplitTransitionPhase.prepare, prepared.phase);
        try std.testing.expectEqual(shard_state_store.SplitPhase.prepare, prepared.source_split_phase);
    }
}

test "db split coordinator rejects mismatched active transition destination" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-split-identity-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-split-identity-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer source.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
    });
    defer std.testing.allocator.free(setup);
    try source.snapshotBuilder().applyBatch(.{
        .group_id = 301,
        .commit_index = 1,
        .entries_bytes = setup,
    });

    {
        var coord = try SyncCoordinator.init(std.testing.allocator, .{
            .transition_id = 302,
            .attempt_epoch = 1,
            .source_root_dir = src_root,
            .dest_root_dir = dst_root,
            .source_group_id = 301,
            .dest_group_id = 303,
            .source_store = &source,
        });
        defer coord.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("split_prepare:302:1:302:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 301,
            .commit_index = 2,
            .entries_bytes = prepare,
        });

        try std.testing.expectError(error.ConflictingSplitTransition, coord.validateTransitionCoordinates(302, 1, 301, 302));
        try std.testing.expectError(error.ConflictingSplitTransition, coord.status());
        try std.testing.expectError(error.ConflictingSplitTransition, coord.prepareSourceSplit("doc:m", "doc:z"));
        try std.testing.expectError(error.ConflictingSplitTransition, coord.startSourceSplit());
        try std.testing.expectError(error.ConflictingSplitTransition, coord.ensureBootstrapped());
        try std.testing.expectError(error.ConflictingSplitTransition, coord.catchUp());
        try std.testing.expectError(error.ConflictingSplitTransition, coord.finalizeSource());
        try std.testing.expectError(error.ConflictingSplitTransition, coord.rollbackSource());
        try std.testing.expectError(error.ConflictingSplitTransition, coord.syncOnce());
        try std.testing.expectError(error.ConflictingSplitTransition, coord.reopenDestination());
    }

    try std.testing.expectError(error.ConflictingSplitTransition, SyncCoordinator.init(std.testing.allocator, .{
        .transition_id = 302,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 301,
        .dest_group_id = 303,
        .source_store = &source,
    }));
}

test "db split coordinator remains closed after failed reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-split-reopen-failure-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const destination_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-split-reopen-failure-destination", .{tmp.sub_path});
    defer std.testing.allocator.free(destination_root);
    const second_source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-split-reopen-failure-source-2", .{tmp.sub_path});
    defer std.testing.allocator.free(second_source_root);
    const second_destination_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-split-reopen-failure-destination-2", .{tmp.sub_path});
    defer std.testing.allocator.free(second_destination_root);

    {
        var coordinator = try SyncCoordinator.init(std.testing.allocator, .{
            .transition_id = 4102,
            .attempt_epoch = 1,
            .source_root_dir = source_root,
            .dest_root_dir = destination_root,
            .source_group_id = 4101,
            .dest_group_id = 4102,
        });
        defer coordinator.deinit();

        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, coordinator.reopenSourceWithAllocator(failing.allocator()));
        try std.testing.expectError(error.SplitCoordinatorSourceClosed, coordinator.status());
        try std.testing.expectError(error.SplitCoordinatorSourceClosed, coordinator.syncOnce());
    }

    {
        var coordinator = try SyncCoordinator.init(std.testing.allocator, .{
            .transition_id = 4202,
            .attempt_epoch = 1,
            .source_root_dir = second_source_root,
            .dest_root_dir = second_destination_root,
            .source_group_id = 4201,
            .dest_group_id = 4202,
        });
        defer coordinator.deinit();

        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, coordinator.reopenDestinationWithAllocator(failing.allocator()));
        try std.testing.expectError(error.SplitCoordinatorDestinationClosed, coordinator.status());
        try std.testing.expectError(error.SplitCoordinatorDestinationClosed, coordinator.syncOnce());
    }
}

test "db merge coordinator bootstraps receiver for donor range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const donor_setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:y={\"v\":\"donor-2\"}") },
        });
        defer std.testing.allocator.free(donor_setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 141,
            .commit_index = 3,
            .entries_bytes = donor_setup,
        });
    }

    {
        var receiver = try Destination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var coord = try MergeCoordinator.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 141,
        .receiver_group_id = 142,
    });
    defer coord.deinit();

    {
        const status = try coord.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.prepare, status.phase);
        try std.testing.expect(!status.receiver_ready_for_reads);
    }

    try coord.acceptDonorRange();
    {
        const status = try coord.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.bootstrap_peer, status.phase);
    }

    {
        const status = try coord.syncOnce();
        try std.testing.expectEqual(range_transition.TransitionPhase.cutover_ready, status.phase);
        try std.testing.expect(status.receiver_ready_for_reads);
        try std.testing.expectEqualStrings("doc:a", coord.receiver.getRange().start);
        try std.testing.expectEqualStrings("doc:z", coord.receiver.getRange().end);
        const donor_doc = (try coord.receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(donor_doc);
        try std.testing.expectEqualStrings("{\"v\":\"donor\"}", donor_doc);
        const receiver_doc = (try coord.receiver.get(std.testing.allocator, "doc:b")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(receiver_doc);
        try std.testing.expectEqualStrings("{\"v\":\"receiver\"}", receiver_doc);
    }

    const catchup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("put:doc:x={\"v\":\"donor-3\"}") },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("del:doc:t") },
    });
    defer std.testing.allocator.free(catchup);
    try coord.donor.snapshotBuilder().applyBatch(.{
        .group_id = 141,
        .commit_index = 5,
        .entries_bytes = catchup,
    });

    {
        const status = try coord.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.replay_deltas, status.phase);
        try std.testing.expect(!status.replay_caught_up);
    }

    {
        const status = try coord.syncOnce();
        try std.testing.expectEqual(range_transition.TransitionPhase.cutover_ready, status.phase);
        try std.testing.expect(status.replay_caught_up);
        try std.testing.expect((try coord.receiver.get(std.testing.allocator, "doc:t")) == null);
        const donor_x = (try coord.receiver.get(std.testing.allocator, "doc:x")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(donor_x);
        try std.testing.expectEqualStrings("{\"v\":\"donor-3\"}", donor_x);
    }

    try coord.reopenDonor();
    try coord.reopenReceiver();
    {
        const status = try coord.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.cutover_ready, status.phase);
        try std.testing.expect(status.receiver_ready_for_reads);
        try std.testing.expectEqual(@as(u64, 5), status.donor_delta_sequence);
        try std.testing.expectEqual(@as(u64, 5), status.receiver_delta_sequence);
    }
}

test "db merge coordinator finalize persists across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-finalize-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-finalize-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();
        const donor_setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(donor_setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 151,
            .commit_index = 2,
            .entries_bytes = donor_setup,
        });
    }

    {
        var receiver = try Destination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
    }

    {
        var coord = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 151,
            .receiver_group_id = 152,
        });
        defer coord.deinit();

        try coord.recordDocIdentityReassignmentOptIn();
        try coord.acceptDonorRange();
        _ = try coord.syncOnce();
        try std.testing.expect(try coord.finalizeMerge());
    }

    {
        var reopened = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 151,
            .receiver_group_id = 152,
        });
        defer reopened.deinit();
        const status = try reopened.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.finalized, status.phase);
        try std.testing.expect(status.receiver_ready_for_reads);
        try std.testing.expect(reopened.allow_doc_identity_reassignment);
        try std.testing.expect(status.allow_doc_identity_reassignment);
    }
}

test "db merge coordinator reassigns receiver identity namespace only after opt-in" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();
    }

    const old_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 191,
        .range_id = 9001,
    };
    const new_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 191,
        .range_id = 9002,
    };
    const receiver_db_options = db_mod.OpenOptions{ .identity_namespace = old_namespace };

    {
        var receiver = try Destination.init(std.testing.allocator, .{
            .root_dir = receiver_root,
            .db = receiver_db_options,
        });
        defer receiver.deinit();
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var coord = try MergeCoordinator.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 190,
        .receiver_group_id = 191,
        .receiver = .{
            .root_dir = receiver_root,
            .db = receiver_db_options,
        },
    });
    defer coord.deinit();

    try std.testing.expectError(error.DocIdentityReassignmentNotAllowed, coord.reassignReceiverIdentityNamespace(new_namespace));

    try coord.recordDocIdentityReassignmentOptIn();
    try coord.reassignReceiverIdentityNamespace(new_namespace);

    const stats = try coord.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
    try std.testing.expectEqual(new_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(new_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(new_namespace.range_id, stats.doc_identity.namespace_range_id);

    var txn = try coord.receiver.db.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(std.testing.allocator, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
    const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:b"), state.canonical_doc_id);
}

test "db merge coordinator opt-in applies configured receiver identity namespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-target-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-target-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();
    }

    const old_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 291,
        .range_id = 9101,
    };
    const target_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 291,
        .range_id = 9102,
    };
    const receiver_db_options = db_mod.OpenOptions{ .identity_namespace = old_namespace };

    {
        var receiver = try Destination.init(std.testing.allocator, .{
            .root_dir = receiver_root,
            .db = receiver_db_options,
        });
        defer receiver.deinit();
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var coord = try MergeCoordinator.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 290,
        .receiver_group_id = 291,
        .receiver = .{
            .root_dir = receiver_root,
            .db = receiver_db_options,
        },
        .receiver_identity_reassignment_namespace = target_namespace,
    });

    try std.testing.expectError(error.DocIdentityReassignmentNotAllowed, coord.acceptDonorRange());
    try std.testing.expect(!coord.allow_doc_identity_reassignment);

    try coord.recordDocIdentityReassignmentOptIn();

    const stats = try coord.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
    try std.testing.expectEqual(target_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(target_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(target_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expect(coord.allow_doc_identity_reassignment);

    {
        var txn = try coord.receiver.db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(std.testing.allocator, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(target_namespace, "doc:b"), state.canonical_doc_id);
    }

    try coord.acceptDonorRange();
    const status = try coord.status();
    try std.testing.expect(status.allow_doc_identity_reassignment);
    try std.testing.expectEqual(target_namespace.table_id, status.receiver_identity_reassignment_namespace_table_id);
    try std.testing.expectEqual(target_namespace.shard_id, status.receiver_identity_reassignment_namespace_shard_id);
    try std.testing.expectEqual(target_namespace.range_id, status.receiver_identity_reassignment_namespace_range_id);
    coord.deinit();

    {
        var reopened = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 290,
            .receiver_group_id = 291,
            .receiver = .{
                .root_dir = receiver_root,
                .db = .{
                    .identity_namespace = target_namespace,
                    .prefer_existing_identity_namespace = true,
                },
            },
            .receiver_identity_reassignment_namespace = target_namespace,
        });
        defer reopened.deinit();
        try std.testing.expect(reopened.allow_doc_identity_reassignment);
        const reopened_stats = try reopened.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
        try std.testing.expectEqual(target_namespace.table_id, reopened_stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(target_namespace.shard_id, reopened_stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(target_namespace.range_id, reopened_stats.doc_identity.namespace_range_id);
    }

    {
        var reopened = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 290,
            .receiver_group_id = 291,
            .receiver = .{
                .root_dir = receiver_root,
                .db = .{
                    .identity_namespace = old_namespace,
                    .prefer_existing_identity_namespace = true,
                },
            },
        });
        defer reopened.deinit();
        try std.testing.expect(reopened.allow_doc_identity_reassignment);
        const reopened_status = try reopened.status();
        try std.testing.expect(reopened_status.allow_doc_identity_reassignment);
        try std.testing.expectEqual(target_namespace.table_id, reopened_status.receiver_identity_reassignment_namespace_table_id);
        try std.testing.expectEqual(target_namespace.shard_id, reopened_status.receiver_identity_reassignment_namespace_shard_id);
        try std.testing.expectEqual(target_namespace.range_id, reopened_status.receiver_identity_reassignment_namespace_range_id);
        const recovered_namespace = reopened.receiver_identity_reassignment_namespace orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(target_namespace.table_id, recovered_namespace.table_id);
        try std.testing.expectEqual(target_namespace.shard_id, recovered_namespace.shard_id);
        try std.testing.expectEqual(target_namespace.range_id, recovered_namespace.range_id);
    }
}

test "db merge coordinator allocates donor docs in receiver identity namespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-identity-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-identity-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();
        const donor_setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(donor_setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 171,
            .commit_index = 2,
            .entries_bytes = donor_setup,
        });
    }

    const receiver_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 172,
        .range_id = 9002,
    };
    const receiver_db_options = db_mod.OpenOptions{ .identity_namespace = receiver_namespace };

    {
        var receiver = try Destination.init(std.testing.allocator, .{
            .root_dir = receiver_root,
            .db = receiver_db_options,
        });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var coord = try MergeCoordinator.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 171,
        .receiver_group_id = 172,
        .receiver = .{
            .root_dir = receiver_root,
            .db = receiver_db_options,
        },
    });
    defer coord.deinit();

    try coord.acceptDonorRange();
    _ = try coord.syncOnce();

    const stats = try coord.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
    try std.testing.expectEqual(receiver_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(receiver_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(receiver_namespace.range_id, stats.doc_identity.namespace_range_id);

    var txn = try coord.receiver.db.core.store.beginProbeTxn();
    defer txn.abort();
    const donor_ordinal = (try doc_identity.lookupOrdinalTxn(std.testing.allocator, &txn, "doc:t")) orelse return error.TestUnexpectedResult;
    const donor_state = (try doc_identity.lookupStateTxn(&txn, donor_ordinal)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(receiver_namespace, "doc:t"), donor_state.canonical_doc_id);
    try std.testing.expect(donor_state.isLive());

    const receiver_ordinal = (try doc_identity.lookupOrdinalTxn(std.testing.allocator, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
    try std.testing.expect(receiver_ordinal != donor_ordinal);

    var filtered = try coord.receiver.db.search(std.testing.allocator, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:t"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
    try std.testing.expectEqualStrings("doc:t", filtered.hits[0].id);
    try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, donor_ordinal), filtered.hits[0].doc_ordinal);

    var receiver_filtered = try coord.receiver.db.search(std.testing.allocator, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:b"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer receiver_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), receiver_filtered.total_hits);
    try std.testing.expectEqualStrings("doc:b", receiver_filtered.hits[0].id);
    try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, receiver_ordinal), receiver_filtered.hits[0].doc_ordinal);
}

test "db merge coordinator rollback restores receiver base range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-rollback-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-rollback-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();
        const donor_setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:y={\"v\":\"donor-2\"}") },
        });
        defer std.testing.allocator.free(donor_setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 161,
            .commit_index = 3,
            .entries_bytes = donor_setup,
        });
    }

    {
        var receiver = try Destination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var coord = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 161,
            .receiver_group_id = 162,
        });
        defer coord.deinit();

        try coord.acceptDonorRange();
        _ = try coord.syncOnce();
        try std.testing.expect(try coord.rollbackMerge());

        const status = try coord.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.rolled_back, status.phase);
        try std.testing.expect(!status.receiver_ready_for_reads);
        try std.testing.expectEqualStrings("doc:a", coord.receiver.getRange().start);
        try std.testing.expectEqualStrings("doc:m", coord.receiver.getRange().end);
        try std.testing.expect((try coord.receiver.get(std.testing.allocator, "doc:t")) == null);
        const receiver_doc = (try coord.receiver.get(std.testing.allocator, "doc:b")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(receiver_doc);
        try std.testing.expectEqualStrings("{\"v\":\"receiver\"}", receiver_doc);
    }

    {
        var reopened = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 161,
            .receiver_group_id = 162,
        });
        defer reopened.deinit();
        const status = try reopened.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.rolled_back, status.phase);
        try std.testing.expectEqualStrings("doc:a", reopened.receiver.getRange().start);
        try std.testing.expectEqualStrings("doc:m", reopened.receiver.getRange().end);
    }
}

test "db merge coordinator rollback reapplies target namespace for persisted reassignment opt-in" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-rollback-reassign-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-rollback-reassign-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    const old_namespace = doc_identity.Namespace{
        .table_id = 10,
        .shard_id = 262,
        .range_id = 9201,
    };
    const target_namespace = doc_identity.Namespace{
        .table_id = 10,
        .shard_id = 262,
        .range_id = 9202,
    };

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();
        const donor_setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(donor_setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 261,
            .commit_index = 2,
            .entries_bytes = donor_setup,
        });
    }

    {
        var receiver = try Destination.init(std.testing.allocator, .{
            .root_dir = receiver_root,
            .db = .{ .identity_namespace = old_namespace },
        });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" }},
        });
    }

    {
        var coord = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 261,
            .receiver_group_id = 262,
            .receiver = .{
                .root_dir = receiver_root,
                .db = .{
                    .identity_namespace = old_namespace,
                    .prefer_existing_identity_namespace = true,
                },
            },
            .receiver_identity_reassignment_namespace = target_namespace,
        });
        defer coord.deinit();

        try coord.recordDocIdentityReassignmentOptIn();
        try coord.acceptDonorRange();
        _ = try coord.syncOnce();
    }

    {
        var reopened = try MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 261,
            .receiver_group_id = 262,
            .receiver = .{
                .root_dir = receiver_root,
                .db = .{
                    .identity_namespace = old_namespace,
                    .prefer_existing_identity_namespace = true,
                },
            },
        });
        defer reopened.deinit();

        try std.testing.expect(reopened.allow_doc_identity_reassignment);
        const recovered_namespace = reopened.receiver_identity_reassignment_namespace orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(target_namespace.table_id, recovered_namespace.table_id);
        try std.testing.expectEqual(target_namespace.shard_id, recovered_namespace.shard_id);
        try std.testing.expectEqual(target_namespace.range_id, recovered_namespace.range_id);
        try std.testing.expect(try reopened.rollbackMerge());

        const status = try reopened.status();
        try std.testing.expectEqual(range_transition.TransitionPhase.rolled_back, status.phase);
        try std.testing.expectEqualStrings("doc:a", reopened.receiver.getRange().start);
        try std.testing.expectEqualStrings("doc:m", reopened.receiver.getRange().end);
        try std.testing.expect((try reopened.receiver.get(std.testing.allocator, "doc:t")) == null);

        const receiver_doc = (try reopened.receiver.get(std.testing.allocator, "doc:b")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(receiver_doc);
        try std.testing.expectEqualStrings("{\"v\":\"receiver\"}", receiver_doc);

        const stats = try reopened.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
        try std.testing.expectEqual(target_namespace.table_id, stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(target_namespace.shard_id, stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(target_namespace.range_id, stats.doc_identity.namespace_range_id);

        var txn = try reopened.receiver.db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(std.testing.allocator, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(target_namespace, "doc:b"), state.canonical_doc_id);
    }
}
