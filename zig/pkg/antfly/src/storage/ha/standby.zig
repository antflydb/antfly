// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Standby-side HA receive/apply progress.
//!
//! A standby durably stores received replication records before applying them,
//! then persists applied and safe-read LSN progress separately. The concrete DB
//! apply implementation plugs in through an idempotent callback; this module owns
//! the ordering, identity validation, and restart/catch-up invariants.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const wal_mod = @import("../wal_runtime.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const platform_sync = @import("antfly_platform").sync;

const progress_magic = [8]u8{ 'A', 'F', 'H', 'A', 'P', 'R', 'G', '\n' };
const progress_version: u16 = 1;
const progress_header_len: usize = 80;

const version_offset: usize = 8;
const header_len_offset: usize = 10;
const cluster_id_offset: usize = 12;
const shard_id_offset: usize = 20;
const table_id_offset: usize = 28;
const timeline_id_offset: usize = 36;
const epoch_offset: usize = 44;
const received_lsn_offset: usize = 52;
const applied_lsn_offset: usize = 60;
const safe_read_lsn_offset: usize = 68;
const header_crc_offset: usize = 76;

var test_path_counter: u64 = 0;

comptime {
    std.debug.assert(header_crc_offset + 4 == progress_header_len);
}

pub const Identity = struct {
    cluster_id: u64,
    shard_id: u64 = 0,
    table_id: u64 = 0,
    timeline_id: u64,
    epoch: u64,
};

pub fn validateIdentity(identity: Identity) !void {
    if (identity.cluster_id == 0) return error.InvalidIdentity;
    if (identity.timeline_id == 0) return error.InvalidIdentity;
    if (identity.epoch == 0) return error.InvalidIdentity;
}

pub const Progress = struct {
    received_lsn: u64 = 0,
    applied_lsn: u64 = 0,
    safe_read_lsn: u64 = 0,

    pub fn nextReceiveLsn(self: Progress) u64 {
        return self.received_lsn + 1;
    }

    pub fn nextApplyLsn(self: Progress) u64 {
        return self.applied_lsn + 1;
    }
};

pub const PromotionRequest = struct {
    new_timeline_id: u64,
    new_epoch: u64,
    required_lsn: ?u64 = null,
    fencing_confirmed: bool = false,
    force: bool = false,
};

pub const PromotionResult = struct {
    switch_lsn: u64,
    old_identity: Identity,
    new_identity: Identity,
    forced: bool,
    data_loss_possible: bool,
};

pub const PromotionHandoff = struct {
    identity: Identity,
    switch_lsn: u64,
    next_lsn: u64,
};

const ProgressRecord = struct {
    identity: Identity,
    progress: Progress,
};

const ProgressReplay = struct {
    identity: ?Identity = null,
    progress: Progress = .{},
};

const TimelineSwitchRecovery = struct {
    identity: Identity,
    progress: Progress,
};

pub const OpenOptions = struct {
    receive_log_options: replication_log.OpenOptions = .{},
    progress_wal_options: wal_mod.WalOptions = .{},
};

pub const ApplyFn = *const fn (ctx: *anyopaque, record: replication_record.RecordView) anyerror!void;

pub const Snapshot = struct {
    identity: Identity,
    progress: Progress,
};

pub const PathMatchResult = enum {
    match,
    receive_log_mismatch,
    progress_wal_mismatch,
};

pub const Standby = struct {
    alloc: Allocator,
    identity_state: Identity,
    receive_log_path: [:0]u8,
    progress_wal_path: [:0]u8,
    receive_log: replication_log.ReplicationLog,
    progress_wal: wal_mod.WAL,
    progress_state: Progress,
    operation_mutex: std.atomic.Mutex = .unlocked,
    state_mutex: std.atomic.Mutex = .unlocked,
    consumed: bool = false,

    pub fn open(
        alloc: Allocator,
        receive_log_path: [*:0]const u8,
        progress_wal_path: [*:0]const u8,
        identity: Identity,
        options: OpenOptions,
    ) !Standby {
        try validateIdentity(identity);

        const owned_receive_log_path = try alloc.dupeZ(u8, std.mem.span(receive_log_path));
        var receive_path_owned_locally = true;
        errdefer if (receive_path_owned_locally) alloc.free(owned_receive_log_path);
        const owned_progress_wal_path = try alloc.dupeZ(u8, std.mem.span(progress_wal_path));
        var progress_path_owned_locally = true;
        errdefer if (progress_path_owned_locally) alloc.free(owned_progress_wal_path);
        var receive_log = try replication_log.ReplicationLog.open(owned_receive_log_path.ptr, options.receive_log_options);
        const progress_wal = wal_mod.WAL.open(owned_progress_wal_path.ptr, options.progress_wal_options) catch |err| {
            receive_log.close();
            return err;
        };
        var standby = Standby{
            .alloc = alloc,
            .identity_state = identity,
            .receive_log_path = owned_receive_log_path,
            .progress_wal_path = owned_progress_wal_path,
            .receive_log = receive_log,
            .progress_wal = progress_wal,
            .progress_state = .{},
        };
        receive_path_owned_locally = false;
        progress_path_owned_locally = false;
        errdefer standby.close();

        const replayed = try standby.replayProgress();
        standby.progress_state = replayed.progress;
        const received_identity = try standby.validateReceivedLog();
        if (received_identity) |received| {
            standby.identity_state = try advanceOpenIdentityFromReceived(standby.identity_state, received);
        }

        const durable_received_lsn = standby.receive_log.lastLsn();
        const replayed_progress = standby.progress_state;
        if (standby.progress_state.received_lsn > durable_received_lsn) {
            const can_recover_truncated_promotion = try standby.progressRecordTargetsTimelineSwitch(.{
                .identity = replayed.identity orelse standby.identity_state,
                .progress = .{
                    .received_lsn = durable_received_lsn,
                    .applied_lsn = durable_received_lsn,
                    .safe_read_lsn = durable_received_lsn,
                },
            });
            if (!can_recover_truncated_promotion) return error.ProgressAheadOfReceivedWal;
        }
        standby.progress_state.received_lsn = durable_received_lsn;
        if (try standby.recoverTimelineSwitch(replayed_progress)) |timeline_recovery| {
            standby.identity_state = timeline_recovery.identity;
            standby.progress_state = timeline_recovery.progress;
        } else if (try standby.recoverBootstrapCheckpoint(replayed_progress)) |checkpoint_progress| {
            standby.progress_state = checkpoint_progress;
        }
        try validateOpenIdentities(replayed.identity, received_identity, standby.identity_state);
        if (standby.progress_state.applied_lsn > standby.progress_state.received_lsn) return error.AppliedAheadOfReceived;
        if (standby.progress_state.safe_read_lsn > standby.progress_state.applied_lsn) return error.SafeReadAheadOfApplied;

        if (standby.progress_state.received_lsn != replayed_progress.received_lsn or
            standby.progress_state.applied_lsn != replayed_progress.applied_lsn or
            standby.progress_state.safe_read_lsn != replayed_progress.safe_read_lsn)
        {
            try standby.persistProgressForIdentity(standby.identity_state, standby.progress_state);
        }

        return standby;
    }

    pub fn close(self: *Standby) void {
        platform_sync.lockYielding(&self.operation_mutex);
        defer self.operation_mutex.unlock();
        if (self.consumed) return;
        const alloc = self.alloc;
        self.progress_wal.close();
        self.receive_log.close();
        alloc.free(self.progress_wal_path);
        alloc.free(self.receive_log_path);
        self.consumed = true;
    }

    /// Returns identity and progress from one synchronization point. Observers
    /// use a short publication lock and never wait behind receive-log I/O,
    /// database apply, or promotion resource transfer.
    pub fn snapshot(self: *const Standby) Snapshot {
        const mutable: *Standby = @constCast(self);
        platform_sync.lockYielding(&mutable.state_mutex);
        defer mutable.state_mutex.unlock();
        return .{
            .identity = mutable.identity_state,
            .progress = mutable.progress_state,
        };
    }

    /// The caller holds the exclusive operation lease; state publication still
    /// uses its independent short lock so observers remain race-free.
    pub fn snapshotLocked(self: *const Standby) Snapshot {
        return self.snapshot();
    }

    fn publishState(self: *Standby, identity: Identity, progress: Progress) void {
        platform_sync.lockYielding(&self.state_mutex);
        defer self.state_mutex.unlock();
        self.identity_state = identity;
        self.progress_state = progress;
    }

    pub fn identitySnapshot(self: *const Standby) Identity {
        return self.snapshot().identity;
    }

    pub fn currentProgress(self: *const Standby) Progress {
        return self.snapshot().progress;
    }

    pub fn nextReceiveLsn(self: *const Standby) u64 {
        return self.snapshot().progress.nextReceiveLsn();
    }

    pub fn nextApplyLsn(self: *const Standby) u64 {
        return self.snapshot().progress.nextApplyLsn();
    }

    /// Compares configured paths while holding the lifetime lease that protects
    /// the owned path buffers from promotion adoption or close.
    pub fn pathsMatch(
        self: *Standby,
        alloc: Allocator,
        io: std.Io,
        receive_log_path: []const u8,
        progress_wal_path: []const u8,
    ) !PathMatchResult {
        try self.lockExclusive();
        defer self.unlockExclusive();
        if (!try fs_paths.pathsReferToSameExistingFile(alloc, io, receive_log_path, self.receive_log_path))
            return .receive_log_mismatch;
        if (!try fs_paths.pathsReferToSameExistingFile(alloc, io, progress_wal_path, self.progress_wal_path))
            return .progress_wal_mismatch;
        return .match;
    }

    pub fn receive(self: *Standby, record: replication_record.Record) !u64 {
        try self.lockExclusive();
        defer self.unlockExclusive();
        return try self.receiveLocked(record);
    }

    /// The caller must hold the exclusive standby operation lease.
    pub fn receiveLocked(self: *Standby, record: replication_record.Record) !u64 {
        if (record.kind == .timeline_switch) {
            return try self.receiveTimelineSwitch(record);
        }

        try self.validateRecord(record);
        if (record.lsn <= self.progress_state.received_lsn) return error.RecordAlreadyReceived;

        const durable_received_lsn = self.receive_log.lastLsn();
        if (record.lsn <= durable_received_lsn) {
            if (record.lsn != self.progress_state.received_lsn + 1) return error.UnexpectedRecordLsn;
            var existing = (try self.receive_log.entryAt(self.alloc, record.lsn)) orelse return error.MissingReceivedRecord;
            defer existing.deinit(self.alloc);
            try self.validateRecord(existing.record);
            if (!recordsEqual(existing.record, record)) return error.ConflictingReceivedRecord;

            var next = self.progress_state;
            next.received_lsn = record.lsn;
            try self.persistProgressForIdentity(self.identity_state, next);
            self.publishState(self.identity_state, next);
            return record.lsn;
        }

        if (record.lsn != self.progress_state.received_lsn + 1) return error.UnexpectedRecordLsn;
        if (record.previous_lsn != self.progress_state.received_lsn) return error.UnexpectedPreviousLsn;

        const lsn = try self.receive_log.append(self.alloc, record);
        var next = self.progress_state;
        next.received_lsn = lsn;
        try self.persistProgressForIdentity(self.identity_state, next);
        self.publishState(self.identity_state, next);
        return lsn;
    }

    fn receiveTimelineSwitch(self: *Standby, record: replication_record.Record) !u64 {
        if (record.cluster_id != self.identity_state.cluster_id) return error.WrongCluster;
        if (!scopeDimensionMatches(self.identity_state.shard_id, record.shard_id)) return error.WrongShard;
        if (!scopeDimensionMatches(self.identity_state.table_id, record.table_id)) return error.WrongTable;
        if (record.timeline_id <= self.identity_state.timeline_id) return error.InvalidTimelineSwitch;
        if (record.epoch <= self.identity_state.epoch) return error.InvalidTimelineSwitch;
        if (record.lsn != self.progress_state.received_lsn + 1) return error.UnexpectedRecordLsn;
        if (record.previous_lsn != self.progress_state.received_lsn) return error.UnexpectedPreviousLsn;
        if (self.progress_state.applied_lsn != self.progress_state.received_lsn or
            self.progress_state.safe_read_lsn != self.progress_state.applied_lsn) return error.TimelineSwitchRequiresAppliedProgress;

        const lsn = try self.receive_log.append(self.alloc, record);
        const next_identity = Identity{
            .cluster_id = record.cluster_id,
            .shard_id = self.identity_state.shard_id,
            .table_id = self.identity_state.table_id,
            .timeline_id = record.timeline_id,
            .epoch = record.epoch,
        };
        const next_progress = Progress{
            .received_lsn = lsn,
            .applied_lsn = lsn,
            .safe_read_lsn = lsn,
        };
        self.persistProgressForIdentity(next_identity, next_progress) catch |err| {
            // The receive log already durably owns the timeline switch. Keep
            // in-memory identity aligned with that authoritative log so a
            // caller cannot append another record on the retired timeline.
            self.publishState(next_identity, next_progress);
            return err;
        };
        self.publishState(next_identity, next_progress);
        return lsn;
    }

    pub fn bootstrapCheckpoint(self: *Standby, checkpoint_lsn: u64, payload: []const u8) !void {
        try self.lockExclusive();
        defer self.unlockExclusive();
        if (checkpoint_lsn == 0) return error.InvalidCheckpointLsn;
        if (self.progress_state.received_lsn != 0 or
            self.progress_state.applied_lsn != 0 or
            self.progress_state.safe_read_lsn != 0 or
            self.receive_log.lastLsn() != 0) return error.StandbyAlreadyBootstrapped;

        _ = try self.receive_log.bootstrapAt(self.alloc, .{
            .kind = .checkpoint,
            .payload_codec = .json,
            .cluster_id = self.identity_state.cluster_id,
            .shard_id = self.identity_state.shard_id,
            .table_id = self.identity_state.table_id,
            .timeline_id = self.identity_state.timeline_id,
            .epoch = self.identity_state.epoch,
            .lsn = checkpoint_lsn,
            .previous_lsn = checkpoint_lsn - 1,
            .payload = payload,
        });

        const next = Progress{
            .received_lsn = checkpoint_lsn,
            .applied_lsn = checkpoint_lsn,
            .safe_read_lsn = checkpoint_lsn,
        };
        self.persistProgressForIdentity(self.identity_state, next) catch |err| {
            // bootstrapAt has already established the durable checkpoint.
            self.publishState(self.identity_state, next);
            return err;
        };
        self.publishState(self.identity_state, next);
    }

    pub fn applyAvailable(self: *Standby, ctx: *anyopaque, apply_fn: ApplyFn) !usize {
        try self.lockExclusive();
        defer self.unlockExclusive();
        return try self.applyAvailableLocked(ctx, apply_fn);
    }

    /// The caller must hold the exclusive standby operation lease.
    pub fn applyAvailableLocked(self: *Standby, ctx: *anyopaque, apply_fn: ApplyFn) !usize {
        const from_lsn = self.progress_state.applied_lsn + 1;
        const entries = try self.receive_log.iterateFrom(self.alloc, from_lsn);
        defer replication_log.freeEntries(self.alloc, entries);

        if (entries.len == 0) {
            if (self.progress_state.applied_lsn < self.progress_state.received_lsn) return error.MissingReceivedRecord;
            return 0;
        }

        var applied_count: usize = 0;
        var expected_lsn = from_lsn;
        for (entries) |entry| {
            if (entry.record.lsn != expected_lsn) return error.MissingReceivedRecord;
            try self.validateRecord(entry.record);
            try apply_fn(ctx, entry.record);

            var next = self.progress_state;
            next.applied_lsn = entry.record.lsn;
            next.safe_read_lsn = entry.record.lsn;
            try self.persistProgressForIdentity(self.identity_state, next);
            self.publishState(self.identity_state, next);

            applied_count += 1;
            expected_lsn += 1;
        }

        return applied_count;
    }

    pub fn promote(self: *Standby, request: PromotionRequest) !PromotionResult {
        try self.lockExclusive();
        defer self.unlockExclusive();
        if (request.new_timeline_id <= self.identity_state.timeline_id) return error.InvalidTimelineSwitch;
        if (request.new_epoch <= self.identity_state.epoch) return error.InvalidTimelineSwitch;
        if (!request.fencing_confirmed and !request.force) return error.FencingRequired;

        const required_lsn = request.required_lsn orelse self.progress_state.received_lsn;
        const data_loss_possible = self.progress_state.received_lsn < required_lsn or
            self.progress_state.applied_lsn < self.progress_state.received_lsn or
            self.progress_state.applied_lsn < required_lsn;
        if (data_loss_possible and !request.force) return error.PromotionRequiresForce;

        const old_identity = self.identity_state;
        const new_identity = Identity{
            .cluster_id = self.identity_state.cluster_id,
            .shard_id = self.identity_state.shard_id,
            .table_id = self.identity_state.table_id,
            .timeline_id = request.new_timeline_id,
            .epoch = request.new_epoch,
        };
        const switch_lsn = self.progress_state.received_lsn + 1;
        const payload = try promotionPayload(self.alloc, old_identity, new_identity, required_lsn, request.force, data_loss_possible);
        defer self.alloc.free(payload);

        _ = try self.receive_log.append(self.alloc, .{
            .kind = .timeline_switch,
            .payload_codec = .json,
            .cluster_id = new_identity.cluster_id,
            .shard_id = new_identity.shard_id,
            .table_id = new_identity.table_id,
            .timeline_id = new_identity.timeline_id,
            .epoch = new_identity.epoch,
            .lsn = switch_lsn,
            .previous_lsn = switch_lsn - 1,
            .payload = payload,
        });

        const promoted_progress = Progress{
            .received_lsn = switch_lsn,
            .applied_lsn = switch_lsn,
            .safe_read_lsn = switch_lsn,
        };
        self.persistProgressForIdentity(new_identity, promoted_progress) catch |err| {
            // The timeline-switch record is authoritative once appended.
            self.publishState(new_identity, promoted_progress);
            return err;
        };
        self.publishState(new_identity, promoted_progress);

        return .{
            .switch_lsn = switch_lsn,
            .old_identity = old_identity,
            .new_identity = new_identity,
            .forced = request.force,
            .data_loss_possible = data_loss_possible,
        };
    }

    pub fn promotedPrimaryHandoff(self: *Standby) !PromotionHandoff {
        try self.lockExclusive();
        defer self.unlockExclusive();
        if (self.progress_state.safe_read_lsn == 0) return error.StandbyNotPromoted;
        if (self.progress_state.applied_lsn != self.progress_state.safe_read_lsn) return error.PromotionNotApplied;

        const switch_lsn = self.progress_state.safe_read_lsn;
        var entry = (try self.receive_log.entryAt(self.alloc, switch_lsn)) orelse return error.MissingReceivedRecord;
        defer entry.deinit(self.alloc);
        if (entry.record.kind != .timeline_switch) return error.StandbyNotPromoted;
        try self.validateRecord(entry.record);

        if (self.progress_state.received_lsn > switch_lsn) {
            try self.receive_log.truncateAfter(switch_lsn);
            const truncated_progress = Progress{
                .received_lsn = switch_lsn,
                .applied_lsn = switch_lsn,
                .safe_read_lsn = switch_lsn,
            };
            self.persistProgressForIdentity(self.identity_state, truncated_progress) catch |err| {
                self.publishState(self.identity_state, truncated_progress);
                return err;
            };
            self.publishState(self.identity_state, truncated_progress);
        } else if (self.progress_state.received_lsn != switch_lsn) {
            return error.PromotionNotApplied;
        }

        return .{
            .identity = self.identity_state,
            .switch_lsn = switch_lsn,
            .next_lsn = switch_lsn + 1,
        };
    }

    pub fn lockExclusive(self: *Standby) !void {
        platform_sync.lockYielding(&self.operation_mutex);
        if (self.consumed) {
            self.operation_mutex.unlock();
            return error.StandbyConsumed;
        }
    }

    pub fn unlockExclusive(self: *Standby) void {
        self.operation_mutex.unlock();
    }

    /// Transfers the receive log after the caller has acquired the exclusive
    /// standby operation lease. The standby remains a valid consumed tombstone
    /// so blocked operations can fail deterministically instead of observing
    /// poisoned memory.
    pub fn consumePromotedReceiveLogLocked(self: *Standby) replication_log.ReplicationLog {
        std.debug.assert(!self.consumed);
        const log = self.receive_log;
        self.progress_wal.close();
        self.alloc.free(self.progress_wal_path);
        self.alloc.free(self.receive_log_path);
        self.consumed = true;
        return log;
    }

    fn validateReceivedLog(self: *Standby) !?Identity {
        const entries = try self.receive_log.iterateFrom(self.alloc, 1);
        defer replication_log.freeEntries(self.alloc, entries);

        var expected_lsn: ?u64 = null;
        var current_identity: ?Identity = null;
        for (entries) |entry| {
            if (expected_lsn) |expected| {
                if (entry.record.lsn != expected) return error.MissingReceivedRecord;
            } else {
                expected_lsn = entry.record.lsn;
            }
            if (entry.record.cluster_id != self.identity_state.cluster_id) return error.WrongCluster;
            if (!scopeDimensionMatches(self.identity_state.shard_id, entry.record.shard_id)) return error.WrongShard;
            if (!scopeDimensionMatches(self.identity_state.table_id, entry.record.table_id)) return error.WrongTable;

            if (current_identity) |current| {
                if (recordMatchesTimeline(entry.record, current)) {
                    expected_lsn.? += 1;
                    continue;
                }
                if (entry.record.kind != .timeline_switch) return error.WrongTimeline;
                if (entry.record.timeline_id <= current.timeline_id) return error.InvalidTimelineSwitch;
                if (entry.record.epoch <= current.epoch) return error.InvalidTimelineSwitch;
            }

            current_identity = .{
                .cluster_id = entry.record.cluster_id,
                .shard_id = self.identity_state.shard_id,
                .table_id = self.identity_state.table_id,
                .timeline_id = entry.record.timeline_id,
                .epoch = entry.record.epoch,
            };
            expected_lsn.? += 1;
        }

        return current_identity;
    }

    fn recoverBootstrapCheckpoint(self: *Standby, replayed_progress: Progress) !?Progress {
        if (replayed_progress.received_lsn != 0 or
            replayed_progress.applied_lsn != 0 or
            replayed_progress.safe_read_lsn != 0) return null;

        const durable_received_lsn = self.receive_log.lastLsn();
        if (durable_received_lsn == 0) return null;

        const entries = try self.receive_log.iterateFrom(self.alloc, 1);
        defer replication_log.freeEntries(self.alloc, entries);
        if (entries.len != 1) return null;

        const checkpoint = entries[0].record;
        if (checkpoint.kind != .checkpoint) return null;
        try self.validateRecord(checkpoint);
        if (checkpoint.lsn != durable_received_lsn) return error.MissingReceivedRecord;
        return Progress{
            .received_lsn = checkpoint.lsn,
            .applied_lsn = checkpoint.lsn,
            .safe_read_lsn = checkpoint.lsn,
        };
    }

    fn recoverTimelineSwitch(self: *Standby, replayed_progress: Progress) !?TimelineSwitchRecovery {
        const durable_received_lsn = self.receive_log.lastLsn();
        if (durable_received_lsn == 0) return null;
        if (replayed_progress.received_lsn >= durable_received_lsn) return null;

        var entry = (try self.receive_log.entryAt(self.alloc, durable_received_lsn)) orelse return error.MissingReceivedRecord;
        defer entry.deinit(self.alloc);
        if (entry.record.kind != .timeline_switch) return null;
        if (entry.record.cluster_id != self.identity_state.cluster_id) return error.WrongCluster;
        if (!scopeDimensionMatches(self.identity_state.shard_id, entry.record.shard_id)) return error.WrongShard;
        if (!scopeDimensionMatches(self.identity_state.table_id, entry.record.table_id)) return error.WrongTable;
        if (entry.record.timeline_id < self.identity_state.timeline_id) return error.WrongTimeline;
        if (entry.record.timeline_id == self.identity_state.timeline_id and entry.record.epoch != self.identity_state.epoch) return error.WrongEpoch;
        if (entry.record.timeline_id > self.identity_state.timeline_id and entry.record.epoch <= self.identity_state.epoch) return error.InvalidTimelineSwitch;
        if (entry.record.previous_lsn != replayed_progress.received_lsn) return error.MissingReceivedRecord;

        return .{
            .identity = .{
                .cluster_id = entry.record.cluster_id,
                .shard_id = self.identity_state.shard_id,
                .table_id = self.identity_state.table_id,
                .timeline_id = entry.record.timeline_id,
                .epoch = entry.record.epoch,
            },
            .progress = .{
                .received_lsn = entry.record.lsn,
                .applied_lsn = entry.record.lsn,
                .safe_read_lsn = entry.record.lsn,
            },
        };
    }

    fn validateRecord(self: *const Standby, record: replication_record.RecordView) !void {
        if (record.cluster_id != self.identity_state.cluster_id) return error.WrongCluster;
        if (!scopeDimensionMatches(self.identity_state.shard_id, record.shard_id)) return error.WrongShard;
        if (!scopeDimensionMatches(self.identity_state.table_id, record.table_id)) return error.WrongTable;
        if (record.timeline_id != self.identity_state.timeline_id) return error.WrongTimeline;
        if (record.epoch != self.identity_state.epoch) return error.WrongEpoch;
    }

    fn replayProgress(self: *Standby) !ProgressReplay {
        const entries = try self.progress_wal.iterateFrom(self.alloc, 1);
        defer {
            for (entries) |entry| self.alloc.free(entry.data);
            self.alloc.free(entries);
        }

        var replay: ProgressReplay = .{};
        var current_identity: ?Identity = null;
        for (entries) |entry| {
            const decoded = try decodeProgressRecord(entry.data);
            if (decoded.identity.cluster_id != self.identity_state.cluster_id) return error.WrongCluster;
            if (decoded.identity.shard_id != self.identity_state.shard_id) return error.WrongShard;
            if (decoded.identity.table_id != self.identity_state.table_id) return error.WrongTable;

            if (current_identity) |current| {
                if (decoded.identity.timeline_id < current.timeline_id) return error.NonMonotonicTimeline;
                if (decoded.identity.timeline_id == current.timeline_id and decoded.identity.epoch != current.epoch) return error.WrongEpoch;
                if (decoded.identity.timeline_id > current.timeline_id and decoded.identity.epoch <= current.epoch) return error.InvalidTimelineSwitch;
            }

            if (decoded.progress.applied_lsn > decoded.progress.received_lsn) return error.AppliedAheadOfReceived;
            if (decoded.progress.safe_read_lsn > decoded.progress.applied_lsn) return error.SafeReadAheadOfApplied;
            if (decoded.progress.received_lsn < replay.progress.received_lsn or
                decoded.progress.applied_lsn < replay.progress.applied_lsn or
                decoded.progress.safe_read_lsn < replay.progress.safe_read_lsn)
            {
                if (!try self.progressRecordTargetsTimelineSwitch(decoded)) return error.NonMonotonicProgress;
            }
            current_identity = decoded.identity;
            replay = .{
                .identity = decoded.identity,
                .progress = decoded.progress,
            };
        }

        return replay;
    }

    fn progressRecordTargetsTimelineSwitch(self: *Standby, decoded: ProgressRecord) !bool {
        if (decoded.progress.received_lsn == 0 or
            decoded.progress.received_lsn != decoded.progress.applied_lsn or
            decoded.progress.received_lsn != decoded.progress.safe_read_lsn) return false;

        var entry = (try self.receive_log.entryAt(self.alloc, decoded.progress.received_lsn)) orelse return false;
        defer entry.deinit(self.alloc);
        return entry.record.kind == .timeline_switch and
            entry.record.cluster_id == decoded.identity.cluster_id and
            entry.record.shard_id == decoded.identity.shard_id and
            entry.record.table_id == decoded.identity.table_id and
            entry.record.timeline_id == decoded.identity.timeline_id and
            entry.record.epoch == decoded.identity.epoch;
    }

    fn persistProgressForIdentity(self: *Standby, identity: Identity, progress: Progress) !void {
        if (progress.applied_lsn > progress.received_lsn) return error.AppliedAheadOfReceived;
        if (progress.safe_read_lsn > progress.applied_lsn) return error.SafeReadAheadOfApplied;

        const encoded = encodeProgress(identity, progress);
        _ = try self.progress_wal.append(&encoded);
    }
};

fn scopeDimensionMatches(expected: u64, actual: u64) bool {
    return expected == 0 or actual == expected;
}

fn recordMatchesTimeline(record: replication_record.RecordView, identity: Identity) bool {
    return record.cluster_id == identity.cluster_id and
        record.timeline_id == identity.timeline_id and
        record.epoch == identity.epoch;
}

fn recordsEqual(left: replication_record.RecordView, right: replication_record.RecordView) bool {
    return left.kind == right.kind and
        left.payload_codec == right.payload_codec and
        left.flags == right.flags and
        left.cluster_id == right.cluster_id and
        left.shard_id == right.shard_id and
        left.table_id == right.table_id and
        left.timeline_id == right.timeline_id and
        left.epoch == right.epoch and
        left.lsn == right.lsn and
        left.previous_lsn == right.previous_lsn and
        left.commit_timestamp_ns == right.commit_timestamp_ns and
        std.mem.eql(u8, left.payload, right.payload);
}

fn validateIdentityMatches(actual: Identity, expected: Identity) !void {
    if (actual.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (actual.shard_id != expected.shard_id) return error.WrongShard;
    if (actual.table_id != expected.table_id) return error.WrongTable;
    if (actual.timeline_id != expected.timeline_id) return error.WrongTimeline;
    if (actual.epoch != expected.epoch) return error.WrongEpoch;
}

fn advanceOpenIdentityFromReceived(expected: Identity, received: Identity) !Identity {
    if (received.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (received.shard_id != expected.shard_id) return error.WrongShard;
    if (received.table_id != expected.table_id) return error.WrongTable;
    if (received.timeline_id < expected.timeline_id) return error.WrongTimeline;
    if (received.timeline_id == expected.timeline_id) {
        if (received.epoch != expected.epoch) return error.WrongEpoch;
        return expected;
    }
    if (received.epoch <= expected.epoch) return error.InvalidTimelineSwitch;
    return received;
}

fn validateOpenIdentities(progress_identity: ?Identity, received_identity: ?Identity, expected: Identity) !void {
    if (received_identity) |received| {
        try validateIdentityMatches(received, expected);
    }

    const progress = progress_identity orelse return;
    if (recordIdentityEquals(progress, expected)) return;
    if (received_identity == null) return validateIdentityMatches(progress, expected);
    if (progress.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (progress.shard_id != expected.shard_id) return error.WrongShard;
    if (progress.table_id != expected.table_id) return error.WrongTable;
    if (progress.timeline_id > expected.timeline_id) return error.WrongTimeline;
    if (progress.epoch > expected.epoch) return error.WrongEpoch;
}

fn recordIdentityEquals(actual: Identity, expected: Identity) bool {
    return actual.cluster_id == expected.cluster_id and
        actual.shard_id == expected.shard_id and
        actual.table_id == expected.table_id and
        actual.timeline_id == expected.timeline_id and
        actual.epoch == expected.epoch;
}

fn promotionPayload(
    alloc: Allocator,
    old_identity: Identity,
    new_identity: Identity,
    required_lsn: u64,
    forced: bool,
    data_loss_possible: bool,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .parent_timeline_id = old_identity.timeline_id,
        .new_timeline_id = new_identity.timeline_id,
        .parent_epoch = old_identity.epoch,
        .new_epoch = new_identity.epoch,
        .required_lsn = required_lsn,
        .forced = forced,
        .data_loss_possible = data_loss_possible,
    }, .{});
}

fn encodeProgress(identity: Identity, progress: Progress) [progress_header_len]u8 {
    var out: [progress_header_len]u8 = undefined;
    @memset(&out, 0);
    @memcpy(out[0..8], &progress_magic);
    std.mem.writeInt(u16, out[version_offset..][0..2], progress_version, .little);
    std.mem.writeInt(u16, out[header_len_offset..][0..2], progress_header_len, .little);
    std.mem.writeInt(u64, out[cluster_id_offset..][0..8], identity.cluster_id, .little);
    std.mem.writeInt(u64, out[shard_id_offset..][0..8], identity.shard_id, .little);
    std.mem.writeInt(u64, out[table_id_offset..][0..8], identity.table_id, .little);
    std.mem.writeInt(u64, out[timeline_id_offset..][0..8], identity.timeline_id, .little);
    std.mem.writeInt(u64, out[epoch_offset..][0..8], identity.epoch, .little);
    std.mem.writeInt(u64, out[received_lsn_offset..][0..8], progress.received_lsn, .little);
    std.mem.writeInt(u64, out[applied_lsn_offset..][0..8], progress.applied_lsn, .little);
    std.mem.writeInt(u64, out[safe_read_lsn_offset..][0..8], progress.safe_read_lsn, .little);
    std.mem.writeInt(u32, out[header_crc_offset..][0..4], Crc32.hash(out[0..header_crc_offset]), .little);
    return out;
}

fn decodeProgress(bytes: []const u8, expected_identity: Identity) !Progress {
    const record = try decodeProgressRecord(bytes);
    try validateIdentityMatches(record.identity, expected_identity);
    return record.progress;
}

fn decodeProgressRecord(bytes: []const u8) !ProgressRecord {
    if (bytes.len < progress_header_len) return error.EndOfStream;
    if (bytes.len != progress_header_len) return error.TrailingBytes;
    if (!std.mem.eql(u8, bytes[0..8], &progress_magic)) return error.InvalidMagic;
    const decoded_version = std.mem.readInt(u16, bytes[version_offset..][0..2], .little);
    if (decoded_version == 0 or decoded_version > progress_version) return error.UnsupportedVersion;
    const decoded_header_len = std.mem.readInt(u16, bytes[header_len_offset..][0..2], .little);
    if (decoded_header_len != progress_header_len) return error.UnsupportedHeaderLength;
    const stored_crc = std.mem.readInt(u32, bytes[header_crc_offset..][0..4], .little);
    if (stored_crc != Crc32.hash(bytes[0..header_crc_offset])) return error.HeaderCrcMismatch;

    const decoded_identity = Identity{
        .cluster_id = std.mem.readInt(u64, bytes[cluster_id_offset..][0..8], .little),
        .shard_id = std.mem.readInt(u64, bytes[shard_id_offset..][0..8], .little),
        .table_id = std.mem.readInt(u64, bytes[table_id_offset..][0..8], .little),
        .timeline_id = std.mem.readInt(u64, bytes[timeline_id_offset..][0..8], .little),
        .epoch = std.mem.readInt(u64, bytes[epoch_offset..][0..8], .little),
    };

    return .{
        .identity = decoded_identity,
        .progress = .{
            .received_lsn = std.mem.readInt(u64, bytes[received_lsn_offset..][0..8], .little),
            .applied_lsn = std.mem.readInt(u64, bytes[applied_lsn_offset..][0..8], .little),
            .safe_read_lsn = std.mem.readInt(u64, bytes[safe_read_lsn_offset..][0..8], .little),
        },
    };
}

fn baseRecord(identity: Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

fn timelineSwitchRecord(identity: Identity, lsn: u64, previous_lsn: u64) replication_record.Record {
    return .{
        .kind = .timeline_switch,
        .payload_codec = .json,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = previous_lsn,
        .payload = "{\"parent_timeline_id\":1,\"new_timeline_id\":2,\"parent_epoch\":1,\"new_epoch\":2,\"required_lsn\":2,\"forced\":false,\"data_loss_possible\":false}",
    };
}

const TestPaths = struct {
    receive_log: [:0]u8,
    progress_wal: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.receive_log);
        alloc.free(self.progress_wal);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const receive_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-standby-" ++ name ++ "-receive-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(receive_raw);
    const progress_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-standby-" ++ name ++ "-progress-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(progress_raw);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), receive_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), progress_raw) catch {};

    return .{
        .receive_log = try alloc.dupeZ(u8, receive_raw),
        .progress_wal = try alloc.dupeZ(u8, progress_raw),
    };
}

const ApplyCapture = struct {
    alloc: Allocator,
    payloads: std.ArrayListUnmanaged([]u8) = .empty,
    fail_at_lsn: u64 = 0,

    fn deinit(self: *ApplyCapture) void {
        for (self.payloads.items) |payload| self.alloc.free(payload);
        self.payloads.deinit(self.alloc);
        self.* = undefined;
    }

    fn apply(ctx: *anyopaque, record: replication_record.RecordView) !void {
        const self: *ApplyCapture = @ptrCast(@alignCast(ctx));
        if (record.lsn == self.fail_at_lsn) return error.IntentionalApplyFailure;
        const owned = try self.alloc.dupe(u8, record.payload);
        errdefer self.alloc.free(owned);
        try self.payloads.append(self.alloc, owned);
    }
};

const TimelineSwitchPayload = struct {
    parent_timeline_id: u64,
    new_timeline_id: u64,
    parent_epoch: u64,
    new_epoch: u64,
    required_lsn: u64,
    forced: bool,
    data_loss_possible: bool,
};

test "storage.ha standby rejects invalid open identity" {
    const alloc = std.testing.allocator;
    const valid = Identity{ .cluster_id = 10, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "invalid-open-identity");
    defer paths.deinit(alloc);

    var zero_cluster = valid;
    zero_cluster.cluster_id = 0;
    try std.testing.expectError(error.InvalidIdentity, Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, zero_cluster, .{}));

    var zero_timeline = valid;
    zero_timeline.timeline_id = 0;
    try std.testing.expectError(error.InvalidIdentity, Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, zero_timeline, .{}));

    var zero_epoch = valid;
    zero_epoch.epoch = 0;
    try std.testing.expectError(error.InvalidIdentity, Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, zero_epoch, .{}));
}

test "storage.ha standby allows whole instance shard table identity" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 0, .table_id = 0, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "whole-instance-identity");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();
    try std.testing.expectEqual(@as(u64, 1), standby.nextReceiveLsn());
}

test "storage.ha standby whole instance identity accepts mixed table records across reopen" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 0, .table_id = 0, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "whole-instance-mixed-records");
    defer paths.deinit(alloc);

    {
        var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer standby.close();

        var first = baseRecord(identity, 1, "one");
        first.shard_id = 20;
        first.table_id = 30;
        var second = baseRecord(identity, 2, "two");
        second.shard_id = 21;
        second.table_id = 31;
        _ = try standby.receive(first);
        _ = try standby.receive(second);

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&capture, ApplyCapture.apply));
    }

    {
        var reopened = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer reopened.close();
        try std.testing.expectEqual(identity, reopened.identitySnapshot());
        try std.testing.expectEqual(@as(u64, 2), reopened.currentProgress().safe_read_lsn);
    }
}

test "storage.ha standby snapshots do not wait behind the operation lease" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "snapshot-publication-lock");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();
    const Worker = struct {
        standby: *Standby,
        started: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            _ = self.standby.snapshot();
            self.done.store(true, .release);
        }
    };

    try standby.lockExclusive();
    var operation_locked = true;
    defer if (operation_locked) standby.unlockExclusive();
    var worker = Worker{ .standby = &standby };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    while (!worker.started.load(.acquire)) std.atomic.spinLoopHint();
    var attempts: usize = 0;
    while (!worker.done.load(.acquire) and attempts < 1_000) : (attempts += 1)
        std.Thread.yield() catch {};
    const completed_while_operation_locked = worker.done.load(.acquire);
    standby.unlockExclusive();
    operation_locked = false;
    thread.join();
    try std.testing.expect(completed_while_operation_locked);
}

test "storage.ha standby path observation is fenced after consumption" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "path-observation");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.testing.expectEqual(
        PathMatchResult.match,
        try standby.pathsMatch(alloc, io_impl.io(), paths.receive_log, paths.progress_wal),
    );
    standby.close();
    try std.testing.expectError(
        error.StandbyConsumed,
        standby.pathsMatch(alloc, io_impl.io(), paths.receive_log, paths.progress_wal),
    );
    try std.testing.expectEqual(identity, standby.snapshot().identity);
}

test "storage.ha standby receives applies and persists progress across reopen" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "reopen");
    defer paths.deinit(alloc);

    {
        var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), standby.nextReceiveLsn());
        _ = try standby.receive(baseRecord(identity, 1, "one"));
        _ = try standby.receive(baseRecord(identity, 2, "two"));

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqual(@as(usize, 2), capture.payloads.items.len);
        try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
        try std.testing.expectEqualStrings("two", capture.payloads.items[1]);
    }

    {
        var reopened = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer reopened.close();
        const progress = reopened.currentProgress();
        try std.testing.expectEqual(@as(u64, 2), progress.received_lsn);
        try std.testing.expectEqual(@as(u64, 2), progress.applied_lsn);
        try std.testing.expectEqual(@as(u64, 2), progress.safe_read_lsn);

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 0), try reopened.applyAvailable(&capture, ApplyCapture.apply));
    }
}

test "storage.ha standby replays received wal after crash before apply" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "catch-up");
    defer paths.deinit(alloc);

    {
        var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer standby.close();
        _ = try standby.receive(baseRecord(identity, 1, "one"));
        _ = try standby.receive(baseRecord(identity, 2, "two"));
    }

    {
        var reopened = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer reopened.close();
        const before = reopened.currentProgress();
        try std.testing.expectEqual(@as(u64, 2), before.received_lsn);
        try std.testing.expectEqual(@as(u64, 0), before.applied_lsn);

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 2), try reopened.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
        try std.testing.expectEqualStrings("two", capture.payloads.items[1]);
    }
}

test "storage.ha standby rejects wrong identity and receive gaps" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "identity");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();

    var wrong_cluster = baseRecord(identity, 1, "wrong");
    wrong_cluster.cluster_id = 99;
    try std.testing.expectError(error.WrongCluster, standby.receive(wrong_cluster));

    var wrong_timeline = baseRecord(identity, 1, "wrong");
    wrong_timeline.timeline_id = 2;
    try std.testing.expectError(error.WrongTimeline, standby.receive(wrong_timeline));

    try std.testing.expectError(error.UnexpectedRecordLsn, standby.receive(baseRecord(identity, 2, "gap")));
    var wrong_previous = baseRecord(identity, 1, "wrong-previous");
    wrong_previous.previous_lsn = 9;
    try std.testing.expectError(error.UnexpectedPreviousLsn, standby.receive(wrong_previous));
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    try std.testing.expectError(error.RecordAlreadyReceived, standby.receive(baseRecord(identity, 1, "duplicate")));
}

test "storage.ha standby follows promoted timeline switch after applying parent timeline" {
    const alloc = std.testing.allocator;
    const parent = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const promoted = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 2, .epoch = 2 };
    const paths = try testPaths(alloc, "follow-timeline");
    defer paths.deinit(alloc);

    {
        var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, parent, .{});
        defer standby.close();

        _ = try standby.receive(baseRecord(parent, 1, "one"));
        _ = try standby.receive(baseRecord(parent, 2, "two"));
        try std.testing.expectError(error.TimelineSwitchRequiresAppliedProgress, standby.receive(timelineSwitchRecord(promoted, 3, 2)));

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&capture, ApplyCapture.apply));

        try std.testing.expectEqual(@as(u64, 3), try standby.receive(timelineSwitchRecord(promoted, 3, 2)));
        try std.testing.expectEqual(promoted.timeline_id, standby.identity_state.timeline_id);
        try std.testing.expectEqual(promoted.epoch, standby.identity_state.epoch);
        try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().applied_lsn);
        try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().safe_read_lsn);

        _ = try standby.receive(baseRecord(promoted, 4, "new-timeline"));
        try std.testing.expectError(error.WrongTimeline, standby.receive(baseRecord(parent, 5, "old-timeline")));
    }

    {
        var reopened = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, promoted, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 4), reopened.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 3), reopened.currentProgress().applied_lsn);

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try reopened.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("new-timeline", capture.payloads.items[0]);
    }
}

test "storage.ha standby receive retry reconciles durable record when progress lags" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "receive-retry");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();

    const first = baseRecord(identity, 1, "one");
    _ = try standby.receive(first);
    standby.progress_state = .{};

    try std.testing.expectEqual(@as(u64, 1), try standby.receive(first));
    try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(u64, 1), standby.receive_log.lastLsn());

    standby.progress_state = .{};
    try std.testing.expectError(error.ConflictingReceivedRecord, standby.receive(baseRecord(identity, 1, "different")));
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
}

test "storage.ha standby does not advance applied lsn when apply fails" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "apply-fail");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    _ = try standby.receive(baseRecord(identity, 2, "two"));

    {
        var capture = ApplyCapture{ .alloc = alloc, .fail_at_lsn = 2 };
        defer capture.deinit();
        try std.testing.expectError(error.IntentionalApplyFailure, standby.applyAvailable(&capture, ApplyCapture.apply));
        const progress = standby.currentProgress();
        try std.testing.expectEqual(@as(u64, 2), progress.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), progress.applied_lsn);
        try std.testing.expectEqual(@as(u64, 1), progress.safe_read_lsn);
    }

    {
        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("two", capture.payloads.items[0]);
        const progress = standby.currentProgress();
        try std.testing.expectEqual(@as(u64, 2), progress.applied_lsn);
        try std.testing.expectEqual(@as(u64, 2), progress.safe_read_lsn);
    }
}

test "storage.ha standby promotion requires fencing and appends timeline switch" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "promote-safe");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    _ = try standby.receive(baseRecord(identity, 2, "two"));

    try std.testing.expectError(error.FencingRequired, standby.promote(.{
        .new_timeline_id = 2,
        .new_epoch = 2,
    }));
    try std.testing.expectError(error.PromotionRequiresForce, standby.promote(.{
        .new_timeline_id = 2,
        .new_epoch = 2,
        .fencing_confirmed = true,
    }));

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&capture, ApplyCapture.apply));

    const result = try standby.promote(.{
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 2,
        .fencing_confirmed = true,
    });
    try std.testing.expectEqual(@as(u64, 3), result.switch_lsn);
    try std.testing.expectEqual(@as(u64, 1), result.old_identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 2), result.new_identity.timeline_id);
    try std.testing.expect(!result.forced);
    try std.testing.expect(!result.data_loss_possible);
    try std.testing.expectEqual(@as(u64, 2), standby.identity_state.timeline_id);
    try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().applied_lsn);

    const entries = try standby.receive_log.iterateFrom(alloc, 3);
    defer replication_log.freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(replication_record.RecordKind.timeline_switch, entries[0].record.kind);
    try std.testing.expectEqual(@as(u64, 2), entries[0].record.timeline_id);
    var switch_payload = try std.json.parseFromSlice(TimelineSwitchPayload, alloc, entries[0].record.payload, .{});
    defer switch_payload.deinit();
    try std.testing.expectEqual(@as(u64, 1), switch_payload.value.parent_timeline_id);
    try std.testing.expectEqual(@as(u64, 2), switch_payload.value.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 1), switch_payload.value.parent_epoch);
    try std.testing.expectEqual(@as(u64, 2), switch_payload.value.new_epoch);
    try std.testing.expectEqual(@as(u64, 2), switch_payload.value.required_lsn);
    try std.testing.expect(!switch_payload.value.forced);
    try std.testing.expect(!switch_payload.value.data_loss_possible);
}

test "storage.ha promoted standby handoff truncates unapplied received records after switch" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "promote-handoff-truncate-unapplied");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    _ = try standby.receive(baseRecord(identity, 2, "two"));

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&capture, ApplyCapture.apply));

    const result = try standby.promote(.{
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 2,
        .fencing_confirmed = true,
    });
    try std.testing.expectEqual(@as(u64, 3), result.switch_lsn);

    _ = try standby.receive(baseRecord(standby.identity_state, 4, "unapplied-after-switch"));
    try std.testing.expectEqual(@as(u64, 4), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().safe_read_lsn);

    const handoff = try standby.promotedPrimaryHandoff();
    try std.testing.expectEqual(@as(u64, 3), handoff.switch_lsn);
    try std.testing.expectEqual(@as(u64, 4), handoff.next_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.receive_log.lastLsn());
}

test "storage.ha standby forced promotion records possible data loss" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "promote-forced");
    defer paths.deinit(alloc);

    var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    _ = try standby.receive(baseRecord(identity, 2, "two"));

    var capture = ApplyCapture{ .alloc = alloc, .fail_at_lsn = 2 };
    defer capture.deinit();
    try std.testing.expectError(error.IntentionalApplyFailure, standby.applyAvailable(&capture, ApplyCapture.apply));
    try std.testing.expectEqual(@as(u64, 2), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().applied_lsn);

    const result = try standby.promote(.{
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 2,
        .force = true,
    });
    try std.testing.expectEqual(@as(u64, 3), result.switch_lsn);
    try std.testing.expect(result.forced);
    try std.testing.expect(result.data_loss_possible);
    try std.testing.expectEqual(@as(u64, 2), standby.identity_state.timeline_id);
    try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.currentProgress().applied_lsn);

    const entries = try standby.receive_log.iterateFrom(alloc, 3);
    defer replication_log.freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(replication_record.RecordKind.timeline_switch, entries[0].record.kind);
    var switch_payload = try std.json.parseFromSlice(TimelineSwitchPayload, alloc, entries[0].record.payload, .{});
    defer switch_payload.deinit();
    try std.testing.expectEqual(@as(u64, 1), switch_payload.value.parent_timeline_id);
    try std.testing.expectEqual(@as(u64, 2), switch_payload.value.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 1), switch_payload.value.parent_epoch);
    try std.testing.expectEqual(@as(u64, 2), switch_payload.value.new_epoch);
    try std.testing.expectEqual(@as(u64, 2), switch_payload.value.required_lsn);
    try std.testing.expect(switch_payload.value.forced);
    try std.testing.expect(switch_payload.value.data_loss_possible);
}

test "storage.ha promoted standby reopens on new timeline and rejects old timeline" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const promoted_identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 2, .epoch = 2 };
    const paths = try testPaths(alloc, "promote-reopen");
    defer paths.deinit(alloc);

    {
        var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer standby.close();
        _ = try standby.receive(baseRecord(identity, 1, "one"));

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&capture, ApplyCapture.apply));
        _ = try standby.promote(.{
            .new_timeline_id = promoted_identity.timeline_id,
            .new_epoch = promoted_identity.epoch,
            .required_lsn = 1,
            .fencing_confirmed = true,
        });
    }

    {
        var recovered = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer recovered.close();
        try std.testing.expectEqual(@as(u64, 2), recovered.identitySnapshot().timeline_id);
        try std.testing.expectEqual(@as(u64, 2), recovered.identitySnapshot().epoch);
        try std.testing.expectEqual(@as(u64, 2), recovered.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 2), recovered.currentProgress().applied_lsn);
        try std.testing.expectError(error.WrongTimeline, recovered.receive(baseRecord(identity, 3, "old-timeline")));
    }

    {
        var reopened = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, promoted_identity, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 2), reopened.identitySnapshot().timeline_id);
        try std.testing.expectEqual(@as(u64, 2), reopened.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 2), reopened.currentProgress().applied_lsn);
        try std.testing.expectError(error.WrongTimeline, reopened.receive(baseRecord(identity, 3, "old-timeline")));

        var new_record = baseRecord(promoted_identity, 3, "new-timeline");
        new_record.previous_lsn = 2;
        try std.testing.expectEqual(@as(u64, 3), try reopened.receive(new_record));
    }
}

test "storage.ha standby recovers timeline switch when promotion progress lags" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 1, .epoch = 1 };
    const promoted_identity = Identity{ .cluster_id = 10, .shard_id = 20, .table_id = 30, .timeline_id = 2, .epoch = 2 };
    const paths = try testPaths(alloc, "promote-crash-recover");
    defer paths.deinit(alloc);

    {
        var standby = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer standby.close();
        _ = try standby.receive(baseRecord(identity, 1, "one"));
        _ = try standby.receive(baseRecord(identity, 2, "two"));

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&capture, ApplyCapture.apply));

        const payload = try promotionPayload(alloc, identity, promoted_identity, 2, false, false);
        defer alloc.free(payload);
        _ = try standby.receive_log.append(alloc, .{
            .kind = .timeline_switch,
            .payload_codec = .json,
            .cluster_id = promoted_identity.cluster_id,
            .shard_id = promoted_identity.shard_id,
            .table_id = promoted_identity.table_id,
            .timeline_id = promoted_identity.timeline_id,
            .epoch = promoted_identity.epoch,
            .lsn = 3,
            .previous_lsn = 2,
            .payload = payload,
        });
    }

    {
        var recovered = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer recovered.close();
        try std.testing.expectEqual(promoted_identity.timeline_id, recovered.identitySnapshot().timeline_id);
        try std.testing.expectEqual(promoted_identity.epoch, recovered.identitySnapshot().epoch);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().applied_lsn);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().safe_read_lsn);
    }

    {
        var recovered = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, promoted_identity, .{});
        defer recovered.close();
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().applied_lsn);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().safe_read_lsn);
    }

    {
        var reopened = try Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, promoted_identity, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 3), reopened.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 3), reopened.currentProgress().applied_lsn);
    }
}
