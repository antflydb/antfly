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

//! Primary-side HA replication coordination.
//!
//! This layer ties the durable HA replication log to durable replication slots.
//! It is intentionally transport-agnostic: HTTP/CLI/operator code can create
//! slots, stream records, submit standby status updates, and ask whether a
//! target LSN satisfies the configured async/remote-write/remote-apply policy.

const std = @import("std");
const Allocator = std.mem.Allocator;
const backup_manifest = @import("backup_manifest.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");

var test_path_counter: u64 = 0;

pub const Identity = standby_mod.Identity;

pub const OpenOptions = struct {
    replication_log_options: replication_log.OpenOptions = .{},
    slot_store_options: slot_store.OpenOptions = .{},
};

pub const AppendOptions = struct {
    kind: replication_record.RecordKind = .batch_mutation,
    payload_codec: replication_record.PayloadCodec = .raw,
    flags: u32 = 0,
    shard_id: ?u64 = null,
    table_id: ?u64 = null,
    commit_timestamp_ns: i64 = 0,
    payload: []const u8 = &.{},
};

pub const BaseBackupStart = struct {
    slot_name: []const u8,
    manifest_id: []const u8,
};

pub const BaseBackupStartResult = struct {
    slot_name: []const u8,
    manifest_id: []const u8,
    backup_lsn: u64,
    start_record_lsn: u64,
};

pub const BaseBackupEndResult = struct {
    backup_lsn: u64,
    end_record_lsn: u64,
    manifest_id: []const u8,
};

pub const DurabilityMode = enum {
    async,
    remote_write,
    remote_apply,
};

pub const StandbySelection = enum {
    any,
    first,
    all,
};

pub const FailurePolicy = enum {
    block,
    fail_closed,
    degrade_to_async,
};

pub const SyncPolicy = struct {
    mode: DurabilityMode = .async,
    selection: StandbySelection = .any,
    required: usize = 1,
    standby_names: []const []const u8 = &.{},
    failure_policy: FailurePolicy = .block,
};

pub const DurabilityStatus = enum {
    satisfied,
    would_block,
    fail_closed,
    degraded_to_async,
};

pub const DurabilityDecision = struct {
    status: DurabilityStatus,
    mode: DurabilityMode,
    selection: StandbySelection,
    target_lsn: u64,
    progress_lsn: u64,
    missing_lsn_count: u64,
    satisfied_count: usize,
    required_count: usize,
    candidate_count: usize,
};

pub const Primary = struct {
    alloc: Allocator,
    identity: Identity,
    log: replication_log.ReplicationLog,
    slots: slot_store.SlotStore,

    pub fn open(
        alloc: Allocator,
        log_path: [*:0]const u8,
        slot_store_path: [*:0]const u8,
        identity: Identity,
        options: OpenOptions,
    ) !Primary {
        try standby_mod.validateIdentity(identity);

        var log = try replication_log.ReplicationLog.open(log_path, options.replication_log_options);
        const slots = slot_store.SlotStore.open(alloc, slot_store_path, options.slot_store_options) catch |err| {
            log.close();
            return err;
        };
        return .{
            .alloc = alloc,
            .identity = identity,
            .log = log,
            .slots = slots,
        };
    }

    pub fn openPromotedFromStandby(
        alloc: Allocator,
        log_path: [*:0]const u8,
        slot_store_path: [*:0]const u8,
        handoff: standby_mod.PromotionHandoff,
        options: OpenOptions,
    ) !Primary {
        var primary = try Primary.open(alloc, log_path, slot_store_path, handoff.identity, options);
        errdefer primary.close();

        try validatePromotedLog(alloc, &primary.log, handoff);

        return primary;
    }

    /// Converts the promoted standby's live receive-log owner into the primary
    /// log owner. The slot store is opened and the handoff is fully validated
    /// before consuming `standby`, so every error leaves the standby usable.
    pub fn adoptPromotedStandby(
        alloc: Allocator,
        standby: *standby_mod.Standby,
        slot_store_path: [*:0]const u8,
        handoff: standby_mod.PromotionHandoff,
        options: OpenOptions,
    ) !Primary {
        if (handoff.switch_lsn == 0) return error.InvalidPromotionHandoff;
        if (handoff.next_lsn != handoff.switch_lsn + 1) return error.InvalidPromotionHandoff;
        if (!std.meta.eql(standby.identity, handoff.identity)) return error.InvalidPromotionHandoff;

        var slots = try slot_store.SlotStore.open(alloc, slot_store_path, options.slot_store_options);
        errdefer slots.close();
        try validatePromotedLog(alloc, &standby.receive_log, handoff);

        const standby_alloc = standby.alloc;
        standby.progress_wal.close();
        const log = standby.receive_log;
        standby_alloc.free(standby.progress_wal_path);
        standby_alloc.free(standby.receive_log_path);
        standby.* = undefined;
        return .{
            .alloc = alloc,
            .identity = handoff.identity,
            .log = log,
            .slots = slots,
        };
    }

    /// Compatibility spelling retained for callers built against the original
    /// HA hardening API. Both names use the same ownership-safe implementation.
    pub fn takePromotedStandby(
        alloc: Allocator,
        standby: *standby_mod.Standby,
        slot_store_path: [*:0]const u8,
        handoff: standby_mod.PromotionHandoff,
        options: OpenOptions,
    ) !Primary {
        return adoptPromotedStandby(alloc, standby, slot_store_path, handoff, options);
    }

    pub fn close(self: *Primary) void {
        self.slots.close();
        self.log.close();
        self.* = undefined;
    }

    pub fn lastLsn(self: *const Primary) u64 {
        return self.log.lastLsn();
    }

    pub fn nextLsn(self: *const Primary) u64 {
        return self.log.nextLsn();
    }

    pub fn append(self: *Primary, options: AppendOptions) !u64 {
        const lsn = self.nextLsn();
        return try self.log.append(self.alloc, .{
            .kind = options.kind,
            .payload_codec = options.payload_codec,
            .flags = options.flags,
            .cluster_id = self.identity.cluster_id,
            .shard_id = options.shard_id orelse self.identity.shard_id,
            .table_id = options.table_id orelse self.identity.table_id,
            .timeline_id = self.identity.timeline_id,
            .epoch = self.identity.epoch,
            .lsn = lsn,
            .previous_lsn = lsn - 1,
            .commit_timestamp_ns = options.commit_timestamp_ns,
            .payload = options.payload,
        });
    }

    pub fn createSlot(self: *Primary, name: []const u8, initial_lsn: u64) !void {
        try validateSlotName(name);
        if (self.slots.get(name) != null) return error.SlotAlreadyExists;
        if (initial_lsn > self.lastLsn()) return error.InitialLsnAheadOfPrimary;
        try self.slots.createOrUpdate(.{
            .name = name,
            .timeline_id = self.identity.timeline_id,
            .restart_lsn = initial_lsn,
            .received_lsn = initial_lsn,
            .applied_lsn = initial_lsn,
            .safe_read_lsn = initial_lsn,
        });
    }

    pub fn beginBaseBackup(self: *Primary, request: BaseBackupStart) !BaseBackupStartResult {
        try validateSlotName(request.slot_name);
        if (request.manifest_id.len == 0) return error.InvalidManifestId;

        if (self.slots.get(request.slot_name)) |state| {
            if (state.timeline_id != self.identity.timeline_id) return error.WrongTimeline;
            if (!state.reseed_required) {
                if (try self.existingBaseBackupStart(request, state)) |existing| return existing;
                if (try self.recoverOrphanedBaseBackupStart(request, state)) |recovered| return recovered;
                if (state.lifecycle == .seeding or state.active) return error.BaseBackupSlotInUse;
            }
        }

        const backup_lsn = self.nextLsn();
        const previous_lsn = backup_lsn - 1;
        try self.reserveBaseBackupSlot(request.slot_name, backup_lsn, previous_lsn);

        const payload = try backupStartPayload(self.alloc, self.identity, request.slot_name, request.manifest_id, backup_lsn);
        defer self.alloc.free(payload);
        const start_lsn = try self.append(.{
            .kind = .backup_start,
            .payload_codec = .json,
            .payload = payload,
        });
        std.debug.assert(start_lsn == backup_lsn);
        return .{
            .slot_name = request.slot_name,
            .manifest_id = request.manifest_id,
            .backup_lsn = backup_lsn,
            .start_record_lsn = start_lsn,
        };
    }

    pub fn endBaseBackup(self: *Primary, manifest: backup_manifest.Manifest) !BaseBackupEndResult {
        try validateBackupManifestIdentity(self.identity, manifest.identity);
        try backup_manifest.validateManifestInput(manifest);
        try self.validateBackupStart(manifest);

        const encoded_manifest = try backup_manifest.encodeAlloc(self.alloc, manifest);
        defer self.alloc.free(encoded_manifest);
        const end_lsn = try self.append(.{
            .kind = .backup_end,
            .payload_codec = .binary,
            .payload = encoded_manifest,
        });
        return .{
            .backup_lsn = manifest.backup_lsn,
            .end_record_lsn = end_lsn,
            .manifest_id = manifest.manifest_id,
        };
    }

    pub fn dropSlot(self: *Primary, name: []const u8) !void {
        try validateSlotName(name);
        try self.slots.drop(name);
    }

    pub fn pauseSlot(self: *Primary, name: []const u8) !void {
        try validateSlotName(name);
        try self.slots.pause(name);
    }

    pub fn resumeSlot(self: *Primary, name: []const u8) !void {
        try validateSlotName(name);
        try self.slots.resumeSlot(name);
    }

    /// Make a base-backup slot streamable only after the caller has verified
    /// that the target restored through the supplied durable progress point.
    pub fn activateSeededSlot(
        self: *Primary,
        name: []const u8,
        timeline_id: u64,
        received_lsn: u64,
        applied_lsn: u64,
        safe_read_lsn: u64,
    ) !void {
        try validateSlotName(name);
        if (timeline_id != self.identity.timeline_id) return error.WrongTimeline;
        const state = self.slots.get(name) orelse return error.SlotNotFound;
        if (state.timeline_id != timeline_id) return error.WrongTimeline;
        if (received_lsn > self.lastLsn()) return error.StandbyAheadOfPrimary;
        if (state.lifecycle == .streaming) {
            if (!state.active or state.reseed_required or
                state.received_lsn < received_lsn or
                state.applied_lsn < applied_lsn or
                state.safe_read_lsn < safe_read_lsn)
            {
                return error.SeedActivationMismatch;
            }
            return;
        }
        const checkpoint_lsn = try self.completedSeedCheckpoint(state, name);
        if (received_lsn != checkpoint_lsn or applied_lsn != checkpoint_lsn or safe_read_lsn != checkpoint_lsn) {
            return error.SeedCheckpointMismatch;
        }
        try self.slots.activateSeeded(name, received_lsn, applied_lsn, safe_read_lsn);
    }

    pub fn markSlotReseedRequired(self: *Primary, name: []const u8) !void {
        try validateSlotName(name);
        try self.slots.markReseedRequired(name);
    }

    pub fn slot(self: *const Primary, name: []const u8) ?slot_store.SlotState {
        return self.slots.get(name);
    }

    pub fn streamFrom(self: *Primary, alloc: Allocator, slot_name: []const u8, from_lsn: u64) ![]replication_log.Entry {
        try validateSlotName(slot_name);
        const state = self.slots.get(slot_name) orelse return error.SlotNotFound;
        if (state.lifecycle == .seeding) return error.SlotSeeding;
        if (!state.active) return error.SlotInactive;
        if (state.reseed_required) return error.SlotRequiresReseed;
        if (state.timeline_id != self.identity.timeline_id) return error.WrongTimeline;
        if (from_lsn < state.restart_lsn) return error.WalNoLongerRetained;
        if (from_lsn > self.nextLsn()) return error.ReplicationStartAheadOfPrimary;
        return try self.log.iterateFrom(alloc, from_lsn);
    }

    pub fn standbyStatusUpdate(
        self: *Primary,
        slot_name: []const u8,
        timeline_id: u64,
        received_lsn: u64,
        applied_lsn: u64,
    ) !void {
        try self.standbyStatusUpdateWithSafeRead(slot_name, timeline_id, received_lsn, applied_lsn, applied_lsn);
    }

    pub fn standbyStatusUpdateWithSafeRead(
        self: *Primary,
        slot_name: []const u8,
        timeline_id: u64,
        received_lsn: u64,
        applied_lsn: u64,
        safe_read_lsn: u64,
    ) !void {
        try validateSlotName(slot_name);
        if (timeline_id != self.identity.timeline_id) return error.WrongTimeline;
        const state = self.slots.get(slot_name) orelse return error.SlotNotFound;
        if (state.lifecycle == .seeding) return error.SlotSeeding;
        if (!state.active) return error.SlotInactive;
        if (state.reseed_required) return error.SlotRequiresReseed;
        if (state.timeline_id != timeline_id) return error.WrongTimeline;
        if (received_lsn > self.lastLsn()) return error.StandbyAheadOfPrimary;
        try self.slots.updateProgress(slot_name, received_lsn, applied_lsn, safe_read_lsn);
    }

    pub fn reportReplicationError(self: *Primary, slot_name: []const u8, last_error: []const u8) !void {
        try validateSlotName(slot_name);
        try self.slots.setLastError(slot_name, last_error);
    }

    pub fn clearReplicationError(self: *Primary, slot_name: []const u8) !void {
        try validateSlotName(slot_name);
        try self.slots.clearLastError(slot_name);
    }

    pub fn retentionSnapshot(
        self: *Primary,
        policy: slot_store.RetentionPolicy,
    ) !slot_store.RetentionSnapshot {
        var snapshot = try self.slots.retentionSnapshotForTimeline(self.lastLsn(), self.identity.timeline_id, policy);
        try self.fillRetainedMetrics(&snapshot);
        while (((policy.max_retained_bytes > 0 and snapshot.retained_byte_count > policy.max_retained_bytes) or
            (policy.max_retained_age_ns > 0 and snapshot.retained_age_ns > policy.max_retained_age_ns)) and
            snapshot.retained_lsn_count > 0)
        {
            const marked = try self.slots.markActiveSlotsAtRestartLsnForTimeline(
                self.identity.timeline_id,
                snapshot.oldest_restart_lsn,
            );
            if (marked == 0) break;
            snapshot = try self.slots.retentionSnapshotForTimeline(self.lastLsn(), self.identity.timeline_id, policy);
            try self.fillRetainedMetrics(&snapshot);
        }
        return snapshot;
    }

    fn fillRetainedMetrics(self: *Primary, snapshot: *slot_store.RetentionSnapshot) !void {
        snapshot.retained_byte_count = try self.retainedByteCount(snapshot.*);
        snapshot.retained_age_ns = try self.retainedAgeNs(snapshot.*);
    }

    fn retainedByteCount(self: *Primary, snapshot: slot_store.RetentionSnapshot) !u64 {
        if (snapshot.retained_lsn_count == 0) return 0;
        const from_lsn = @max(snapshot.oldest_restart_lsn, 1);
        return try self.log.encodedByteCount(from_lsn, snapshot.primary_lsn);
    }

    fn retainedAgeNs(self: *Primary, snapshot: slot_store.RetentionSnapshot) !u64 {
        if (snapshot.retained_lsn_count == 0) return 0;
        const from_lsn = @max(snapshot.oldest_restart_lsn, 1);
        return try self.log.retainedAgeNs(from_lsn, snapshot.primary_lsn);
    }

    pub fn evaluateDurability(self: *const Primary, target_lsn: u64, policy: SyncPolicy) !DurabilityDecision {
        if (target_lsn > self.lastLsn()) return error.TargetAheadOfPrimary;
        return self.evaluateDurabilityAt(target_lsn, policy);
    }

    pub fn evaluateAppendDurability(self: *const Primary, target_lsn: u64, policy: SyncPolicy) !DurabilityDecision {
        if (target_lsn != self.nextLsn()) return error.TargetAheadOfPrimary;
        return self.evaluateDurabilityAt(target_lsn, policy);
    }

    fn evaluateDurabilityAt(self: *const Primary, target_lsn: u64, policy: SyncPolicy) !DurabilityDecision {
        if (policy.mode == .async) {
            return .{
                .status = .satisfied,
                .mode = .async,
                .selection = policy.selection,
                .target_lsn = target_lsn,
                .progress_lsn = target_lsn,
                .missing_lsn_count = 0,
                .satisfied_count = 0,
                .required_count = 0,
                .candidate_count = 0,
            };
        }
        try validateSyncPolicyNames(policy.standby_names);
        if (policy.required == 0) return error.InvalidSyncPolicy;
        if (policy.selection != .all and policy.required > policy.standby_names.len) return error.InvalidSyncPolicy;
        if (policy.standby_names.len == 0) return decisionForUnsatisfied(target_lsn, policy, 0, policy.required, 0);

        const counts = switch (policy.selection) {
            .any => self.evaluateAny(target_lsn, policy),
            .first => self.evaluateFirst(target_lsn, policy),
            .all => self.evaluateAll(target_lsn, policy),
        };

        const satisfied = counts.satisfied_count >= counts.required_count;
        const progress_lsn = if (satisfied) target_lsn else counts.progress_lsn;
        return .{
            .status = if (satisfied) .satisfied else statusForFailurePolicy(policy.failure_policy),
            .mode = policy.mode,
            .selection = policy.selection,
            .target_lsn = target_lsn,
            .progress_lsn = progress_lsn,
            .missing_lsn_count = missingLsnCount(target_lsn, progress_lsn),
            .satisfied_count = counts.satisfied_count,
            .required_count = counts.required_count,
            .candidate_count = counts.candidate_count,
        };
    }

    fn evaluateAny(self: *const Primary, target_lsn: u64, policy: SyncPolicy) Counts {
        var counts = Counts{
            .required_count = policy.required,
        };
        for (policy.standby_names) |name| {
            const state = self.eligibleSlot(name) orelse continue;
            counts.candidate_count += 1;
            if (slotSatisfies(state, target_lsn, policy.mode)) counts.satisfied_count += 1;
        }
        counts.progress_lsn = self.anyProgressLsn(policy);
        return counts;
    }

    fn anyProgressLsn(self: *const Primary, policy: SyncPolicy) u64 {
        if (policy.required == 0) return 0;

        var best: u64 = 0;
        for (policy.standby_names) |candidate_name| {
            const candidate = self.eligibleSlot(candidate_name) orelse continue;
            const candidate_progress = progressForMode(candidate, policy.mode);
            var at_or_above: usize = 0;

            for (policy.standby_names) |name| {
                const state = self.eligibleSlot(name) orelse continue;
                if (progressForMode(state, policy.mode) >= candidate_progress) at_or_above += 1;
            }

            if (at_or_above >= policy.required) best = @max(best, candidate_progress);
        }
        return best;
    }

    fn evaluateFirst(self: *const Primary, target_lsn: u64, policy: SyncPolicy) Counts {
        var counts = Counts{
            .required_count = policy.required,
        };
        for (policy.standby_names) |name| {
            const state = self.eligibleSlot(name) orelse continue;
            counts.candidate_count += 1;
            counts.progress_lsn = counts.progressFloor(progressForMode(state, policy.mode));
            if (slotSatisfies(state, target_lsn, policy.mode)) counts.satisfied_count += 1;
            if (counts.candidate_count == policy.required) break;
        }
        return counts;
    }

    fn evaluateAll(self: *const Primary, target_lsn: u64, policy: SyncPolicy) Counts {
        var counts = Counts{
            .required_count = policy.standby_names.len,
        };
        for (policy.standby_names) |name| {
            const state = self.eligibleSlot(name) orelse continue;
            counts.candidate_count += 1;
            counts.progress_lsn = counts.progressFloor(progressForMode(state, policy.mode));
            if (slotSatisfies(state, target_lsn, policy.mode)) counts.satisfied_count += 1;
        }
        return counts;
    }

    fn eligibleSlot(self: *const Primary, name: []const u8) ?slot_store.SlotState {
        const state = self.slots.get(name) orelse return null;
        if (state.lifecycle == .seeding) return null;
        if (!state.active or state.reseed_required) return null;
        if (state.timeline_id != self.identity.timeline_id) return null;
        return state;
    }

    fn reserveBaseBackupSlot(self: *Primary, slot_name: []const u8, backup_lsn: u64, previous_lsn: u64) !void {
        try validateSlotName(slot_name);
        if (self.slots.get(slot_name)) |state| {
            if (state.timeline_id != self.identity.timeline_id) return error.WrongTimeline;
            if (!state.reseed_required and (state.lifecycle == .seeding or state.active)) return error.BaseBackupSlotInUse;
        }

        try self.slots.createOrUpdate(.{
            .name = slot_name,
            .timeline_id = self.identity.timeline_id,
            .restart_lsn = backup_lsn,
            .received_lsn = previous_lsn,
            .applied_lsn = previous_lsn,
            .safe_read_lsn = previous_lsn,
            .active = false,
            .lifecycle = .seeding,
        });
    }

    fn validateBackupStart(self: *Primary, manifest: backup_manifest.Manifest) !void {
        if (manifest.backup_lsn > self.lastLsn()) return error.BackupStartNotDurable;
        if (manifest.checkpoint_lsn > self.lastLsn()) return error.BackupCheckpointNotDurable;
        var entry = (try self.log.entryAt(self.alloc, manifest.backup_lsn)) orelse return error.BackupStartNotFound;
        defer entry.deinit(self.alloc);
        if (entry.record.kind != .backup_start) return error.BackupStartNotFound;
        if (entry.record.payload_codec != .json) return error.BackupStartMismatch;

        var parsed = std.json.parseFromSlice(BackupStartPayload, self.alloc, entry.record.payload, .{}) catch return error.BackupStartMismatch;
        defer parsed.deinit();

        const start = parsed.value;
        if (start.cluster_id != self.identity.cluster_id) return error.BackupStartMismatch;
        if (start.shard_id != self.identity.shard_id) return error.BackupStartMismatch;
        if (start.table_id != self.identity.table_id) return error.BackupStartMismatch;
        if (start.timeline_id != self.identity.timeline_id) return error.BackupStartMismatch;
        if (start.epoch != self.identity.epoch) return error.BackupStartMismatch;
        if (start.backup_lsn != manifest.backup_lsn) return error.BackupStartMismatch;
        if (!std.mem.eql(u8, start.manifest_id, manifest.manifest_id)) return error.BackupStartMismatch;
        try self.validateBackupSlotRetention(start);
    }

    fn validateBackupSlotRetention(self: *Primary, start: BackupStartPayload) !void {
        const slot_state = self.slots.get(start.slot_name) orelse return error.BackupSlotNotFound;
        if (slot_state.lifecycle != .seeding or slot_state.active or slot_state.reseed_required) return error.BackupSlotNotRetained;
        if (slot_state.timeline_id != self.identity.timeline_id) return error.BackupSlotNotRetained;
        if (slot_state.restart_lsn > start.backup_lsn) return error.BackupSlotNotRetained;
    }

    fn existingBaseBackupStart(self: *Primary, request: BaseBackupStart, state: slot_store.SlotState) !?BaseBackupStartResult {
        if (state.lifecycle != .seeding or state.active or state.restart_lsn == 0) return null;

        var entry = (try self.log.entryAt(self.alloc, state.restart_lsn)) orelse return null;
        defer entry.deinit(self.alloc);
        if (entry.record.kind != .backup_start or entry.record.payload_codec != .json) return null;

        var parsed = std.json.parseFromSlice(BackupStartPayload, self.alloc, entry.record.payload, .{}) catch return null;
        defer parsed.deinit();

        const start = parsed.value;
        if (start.cluster_id != self.identity.cluster_id) return null;
        if (start.shard_id != self.identity.shard_id) return null;
        if (start.table_id != self.identity.table_id) return null;
        if (start.timeline_id != self.identity.timeline_id) return null;
        if (start.epoch != self.identity.epoch) return null;
        if (start.backup_lsn != state.restart_lsn) return null;
        if (!std.mem.eql(u8, start.slot_name, request.slot_name)) return null;
        if (!std.mem.eql(u8, start.manifest_id, request.manifest_id)) return null;

        return .{
            .slot_name = request.slot_name,
            .manifest_id = request.manifest_id,
            .backup_lsn = start.backup_lsn,
            .start_record_lsn = entry.wal_lsn,
        };
    }

    fn recoverOrphanedBaseBackupStart(self: *Primary, request: BaseBackupStart, state: slot_store.SlotState) !?BaseBackupStartResult {
        if (state.lifecycle != .seeding or state.active or state.reseed_required or state.restart_lsn == 0 or
            state.received_lsn != state.restart_lsn - 1 or
            state.applied_lsn != state.restart_lsn - 1 or
            state.safe_read_lsn != state.restart_lsn - 1)
            return null;

        var backup_lsn = state.restart_lsn;
        const last_lsn = self.lastLsn();
        if (backup_lsn <= last_lsn) {
            var occupied = (try self.log.entryAt(self.alloc, backup_lsn)) orelse return null;
            defer occupied.deinit(self.alloc);
            if (occupied.record.kind == .backup_start) return null;

            // The durable slot reservation survived, but another WAL record
            // consumed its LSN before capture retried. No backup_start exists,
            // so advance the inactive seeding reservation to the current WAL
            // boundary and publish the start record there.
            backup_lsn = self.nextLsn();
            try self.slots.createOrUpdate(.{
                .name = request.slot_name,
                .timeline_id = self.identity.timeline_id,
                .restart_lsn = backup_lsn,
                .received_lsn = last_lsn,
                .applied_lsn = last_lsn,
                .safe_read_lsn = last_lsn,
                .active = false,
                .lifecycle = .seeding,
            });
        } else if (backup_lsn != last_lsn +| 1) {
            return null;
        }

        const payload = try backupStartPayload(self.alloc, self.identity, request.slot_name, request.manifest_id, backup_lsn);
        defer self.alloc.free(payload);
        const start_lsn = try self.append(.{
            .kind = .backup_start,
            .payload_codec = .json,
            .payload = payload,
        });
        if (start_lsn != backup_lsn) return error.BackupStartMismatch;
        return .{
            .slot_name = request.slot_name,
            .manifest_id = request.manifest_id,
            .backup_lsn = backup_lsn,
            .start_record_lsn = start_lsn,
        };
    }

    fn completedSeedCheckpoint(self: *Primary, state: slot_store.SlotState, slot_name: []const u8) !u64 {
        if (state.lifecycle != .seeding) return error.SlotNotSeeding;
        var start_entry = (try self.log.entryAt(self.alloc, state.restart_lsn)) orelse return error.BackupStartNotFound;
        defer start_entry.deinit(self.alloc);
        if (start_entry.record.kind != .backup_start or start_entry.record.payload_codec != .json) return error.BackupStartNotFound;

        var parsed = std.json.parseFromSlice(BackupStartPayload, self.alloc, start_entry.record.payload, .{}) catch
            return error.BackupStartMismatch;
        defer parsed.deinit();
        const start = parsed.value;
        if (start.cluster_id != self.identity.cluster_id) return error.BackupStartMismatch;
        if (start.shard_id != self.identity.shard_id) return error.BackupStartMismatch;
        if (start.table_id != self.identity.table_id) return error.BackupStartMismatch;
        if (start.timeline_id != self.identity.timeline_id) return error.BackupStartMismatch;
        if (start.epoch != self.identity.epoch) return error.BackupStartMismatch;
        if (!std.mem.eql(u8, start.slot_name, slot_name)) return error.BackupStartMismatch;
        if (start.backup_lsn != state.restart_lsn) return error.BackupStartMismatch;

        const entries = try self.log.iterateFrom(self.alloc, start.backup_lsn + 1);
        defer replication_log.freeEntries(self.alloc, entries);
        for (entries) |entry| {
            if (entry.record.kind != .backup_end or entry.record.payload_codec != .binary) continue;
            const manifest = backup_manifest.decodeAlloc(self.alloc, entry.record.payload) catch return error.BackupEndMismatch;
            defer backup_manifest.freeDecoded(self.alloc, manifest);
            if (manifest.backup_lsn != start.backup_lsn) continue;
            if (!std.mem.eql(u8, manifest.manifest_id, start.manifest_id)) continue;
            try validateBackupManifestIdentity(self.identity, manifest.identity);
            return manifest.checkpoint_lsn;
        }
        return error.SeedBackupNotComplete;
    }
};

fn validateSlotName(name: []const u8) !void {
    if (!validation.isIdentifier(name)) return error.InvalidSlotName;
}

fn validateSyncPolicyNames(names: []const []const u8) !void {
    for (names) |name| {
        if (!validation.isIdentifier(name)) return error.InvalidSyncPolicy;
    }
}

const BackupStartPayload = struct {
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    slot_name: []const u8,
    manifest_id: []const u8,
    backup_lsn: u64,
};

fn validateBackupManifestIdentity(expected: Identity, actual: Identity) !void {
    if (actual.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (actual.shard_id != expected.shard_id) return error.WrongShard;
    if (actual.table_id != expected.table_id) return error.WrongTable;
    if (actual.timeline_id != expected.timeline_id) return error.WrongTimeline;
    if (actual.epoch != expected.epoch) return error.WrongEpoch;
}

fn validateRecordIdentity(expected: Identity, actual: replication_record.RecordView) !void {
    if (actual.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (actual.shard_id != expected.shard_id) return error.WrongShard;
    if (actual.table_id != expected.table_id) return error.WrongTable;
    if (actual.timeline_id != expected.timeline_id) return error.WrongTimeline;
    if (actual.epoch != expected.epoch) return error.WrongEpoch;
}

fn validatePromotedLog(
    alloc: Allocator,
    log: *replication_log.ReplicationLog,
    handoff: standby_mod.PromotionHandoff,
) !void {
    if (handoff.switch_lsn == 0) return error.InvalidPromotionHandoff;
    if (handoff.next_lsn != handoff.switch_lsn + 1) return error.InvalidPromotionHandoff;
    if (log.lastLsn() != handoff.switch_lsn) return error.PromotedLogMismatch;
    var switch_entry = (try log.entryAt(alloc, handoff.switch_lsn)) orelse return error.MissingPromotionSwitch;
    defer switch_entry.deinit(alloc);
    if (switch_entry.record.kind != .timeline_switch) return error.MissingPromotionSwitch;
    try validateRecordIdentity(handoff.identity, switch_entry.record);
    if (log.nextLsn() != handoff.next_lsn) return error.PromotedLogMismatch;
}

fn backupStartPayload(
    alloc: Allocator,
    identity: Identity,
    slot_name: []const u8,
    manifest_id: []const u8,
    backup_lsn: u64,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .slot_name = slot_name,
        .manifest_id = manifest_id,
        .backup_lsn = backup_lsn,
    }, .{});
}

const Counts = struct {
    satisfied_count: usize = 0,
    required_count: usize = 0,
    candidate_count: usize = 0,
    progress_lsn: u64 = 0,

    fn progressFloor(self: Counts, progress_lsn: u64) u64 {
        if (self.candidate_count == 1) return progress_lsn;
        return @min(self.progress_lsn, progress_lsn);
    }
};

fn slotSatisfies(state: slot_store.SlotState, target_lsn: u64, mode: DurabilityMode) bool {
    return switch (mode) {
        .async => true,
        .remote_write => state.received_lsn >= target_lsn,
        .remote_apply => state.applied_lsn >= target_lsn,
    };
}

fn progressForMode(state: slot_store.SlotState, mode: DurabilityMode) u64 {
    return switch (mode) {
        .async => state.applied_lsn,
        .remote_write => state.received_lsn,
        .remote_apply => state.applied_lsn,
    };
}

fn missingLsnCount(target_lsn: u64, progress_lsn: u64) u64 {
    return target_lsn -| @min(target_lsn, progress_lsn);
}

fn statusForFailurePolicy(policy: FailurePolicy) DurabilityStatus {
    return switch (policy) {
        .block => .would_block,
        .fail_closed => .fail_closed,
        .degrade_to_async => .degraded_to_async,
    };
}

fn decisionForUnsatisfied(
    target_lsn: u64,
    policy: SyncPolicy,
    satisfied_count: usize,
    required_count: usize,
    candidate_count: usize,
) DurabilityDecision {
    return .{
        .status = statusForFailurePolicy(policy.failure_policy),
        .mode = policy.mode,
        .selection = policy.selection,
        .target_lsn = target_lsn,
        .progress_lsn = 0,
        .missing_lsn_count = target_lsn,
        .satisfied_count = satisfied_count,
        .required_count = required_count,
        .candidate_count = candidate_count,
    };
}

const TestPaths = struct {
    log: [:0]u8,
    slots: [:0]u8,
    standby_progress: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.log);
        alloc.free(self.slots);
        alloc.free(self.standby_progress);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const log_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-primary-" ++ name ++ "-log-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(log_raw);
    const slots_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-primary-" ++ name ++ "-slots-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(slots_raw);
    const standby_progress_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-primary-" ++ name ++ "-standby-progress-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(standby_progress_raw);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), log_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), slots_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress_raw) catch {};

    return .{
        .log = try alloc.dupeZ(u8, log_raw),
        .slots = try alloc.dupeZ(u8, slots_raw),
        .standby_progress = try alloc.dupeZ(u8, standby_progress_raw),
    };
}

fn testIdentity() Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn noOpApply(_: *anyopaque, _: replication_record.RecordView) anyerror!void {}

test "storage.ha primary rejects invalid open identity" {
    const alloc = std.testing.allocator;
    const valid = testIdentity();
    const paths = try testPaths(alloc, "invalid-open-identity");
    defer paths.deinit(alloc);

    var zero_cluster = valid;
    zero_cluster.cluster_id = 0;
    try std.testing.expectError(error.InvalidIdentity, Primary.open(alloc, paths.log.ptr, paths.slots.ptr, zero_cluster, .{}));

    var zero_timeline = valid;
    zero_timeline.timeline_id = 0;
    try std.testing.expectError(error.InvalidIdentity, Primary.open(alloc, paths.log.ptr, paths.slots.ptr, zero_timeline, .{}));

    var zero_epoch = valid;
    zero_epoch.epoch = 0;
    try std.testing.expectError(error.InvalidIdentity, Primary.open(alloc, paths.log.ptr, paths.slots.ptr, zero_epoch, .{}));
}

test "storage.ha primary allows whole instance shard table identity" {
    const alloc = std.testing.allocator;
    const identity = Identity{ .cluster_id = 100, .shard_id = 0, .table_id = 0, .timeline_id = 1, .epoch = 1 };
    const paths = try testPaths(alloc, "whole-instance-identity");
    defer paths.deinit(alloc);

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try std.testing.expectEqual(@as(u64, 1), primary.nextLsn());
}

test "storage.ha primary appends streams and persists standby status" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "stream");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    {
        var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
        defer primary.close();
        try primary.createSlot("standby-a", 0);
        try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));
        try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "two" }));

        const entries = try primary.streamFrom(alloc, "standby-a", 1);
        defer replication_log.freeEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 2), entries.len);
        try std.testing.expectEqualStrings("one", entries[0].record.payload);
        try std.testing.expectEqualStrings("two", entries[1].record.payload);

        try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 1);
    }

    {
        var reopened = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 2), reopened.lastLsn());
        const slot = reopened.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
        try std.testing.expectEqual(@as(u64, 2), slot.restart_lsn);
    }
}

test "storage.ha primary opens from promoted standby handoff and continues writes" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "promoted-handoff");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const promoted_identity = Identity{
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = 2,
        .epoch = 2,
    };

    var ctx: u8 = 0;
    const handoff = blk: {
        var standby = try standby_mod.Standby.open(alloc, paths.log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        try std.testing.expectError(error.StandbyNotPromoted, standby.promotedPrimaryHandoff());

        _ = try standby.receive(.{
            .kind = .batch_mutation,
            .cluster_id = identity.cluster_id,
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
            .timeline_id = identity.timeline_id,
            .epoch = identity.epoch,
            .lsn = 1,
            .previous_lsn = 0,
            .payload = "before-promotion",
        });
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&ctx, noOpApply));

        const promotion = try standby.promote(.{
            .new_timeline_id = promoted_identity.timeline_id,
            .new_epoch = promoted_identity.epoch,
            .required_lsn = 1,
            .fencing_confirmed = true,
        });
        try std.testing.expectEqual(@as(u64, 2), promotion.switch_lsn);

        break :blk try standby.promotedPrimaryHandoff();
    };

    var primary = try Primary.openPromotedFromStandby(alloc, paths.log.ptr, paths.slots.ptr, handoff, .{});
    defer primary.close();
    try std.testing.expectEqual(promoted_identity, primary.identity);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 3), primary.nextLsn());

    const appended_lsn = try primary.append(.{ .payload = "after-promotion" });
    try std.testing.expectEqual(@as(u64, 3), appended_lsn);
    var appended = (try primary.log.entryAt(alloc, appended_lsn)) orelse return error.TestExpectedEqual;
    defer appended.deinit(alloc);
    try validateRecordIdentity(promoted_identity, appended.record);
    try std.testing.expectEqual(@as(u64, 2), appended.record.previous_lsn);
    try std.testing.expectEqualStrings("after-promotion", appended.record.payload);
}

test "storage.ha primary rejects duplicate slot creation without regressing progress" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "duplicate-slot");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    try primary.createSlot("standby-a", 0);
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 2);

    try std.testing.expectError(error.SlotAlreadyExists, primary.createSlot("standby-a", 0));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 2), slot.restart_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);
}

test "storage.ha primary rejects future slot creation at storage boundary" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "future-slot");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });

    try std.testing.expectError(error.InitialLsnAheadOfPrimary, primary.createSlot("standby-a", 2));
    try std.testing.expect(primary.slot("standby-a") == null);

    const names = [_][]const u8{"standby-a"};
    const decision = try primary.evaluateDurability(1, .{
        .mode = .remote_write,
        .selection = .any,
        .required = 1,
        .standby_names = &names,
        .failure_policy = .fail_closed,
    });
    try std.testing.expectEqual(DurabilityStatus.fail_closed, decision.status);
    try std.testing.expectEqual(@as(usize, 0), decision.satisfied_count);
}

test "storage.ha primary rejects invalid slot and sync standby identifiers" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-identifiers");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });

    try std.testing.expectError(error.InvalidSlotName, primary.createSlot("standby bad", 0));
    try std.testing.expectError(error.InvalidSlotName, primary.createSlot("standby/a", 0));
    try std.testing.expectError(error.InvalidSlotName, primary.beginBaseBackup(.{
        .slot_name = " standby-a",
        .manifest_id = "manifest-1",
    }));
    try std.testing.expectError(error.InvalidSlotName, primary.streamFrom(alloc, "standby/a", 1));
    try std.testing.expectError(
        error.InvalidSlotName,
        primary.standbyStatusUpdate("standby bad", identity.timeline_id, 1, 1),
    );

    const invalid_sync_names = [_][]const u8{ "standby-a", "standby bad" };
    try std.testing.expectError(error.InvalidSyncPolicy, primary.evaluateDurability(1, .{
        .mode = .remote_write,
        .selection = .any,
        .required = 1,
        .standby_names = &invalid_sync_names,
    }));
}

test "storage.ha primary begins base backup with slot retention pin" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-start");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "before-backup" });

    const started = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectEqualStrings("standby-a", started.slot_name);
    try std.testing.expectEqualStrings("manifest-1", started.manifest_id);
    try std.testing.expectEqual(@as(u64, 2), started.backup_lsn);
    try std.testing.expectEqual(@as(u64, 2), started.start_record_lsn);

    const retried = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectEqual(started.backup_lsn, retried.backup_lsn);
    try std.testing.expectEqual(started.start_record_lsn, retried.start_record_lsn);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try std.testing.expectError(error.BaseBackupSlotInUse, primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-2",
    }));

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(slot_store.SlotLifecycle.seeding, slot.lifecycle);
    try std.testing.expect(!slot.active);
    try std.testing.expectEqual(@as(u64, 2), slot.restart_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);

    const snapshot = try primary.retentionSnapshot(.{});
    try std.testing.expectEqual(@as(u64, 2), snapshot.oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 1), snapshot.retained_lsn_count);
    try std.testing.expectEqual(@as(usize, 0), snapshot.active_slots);
    try std.testing.expect(snapshot.retained_byte_count > 0);

    try std.testing.expectError(error.SlotSeeding, primary.streamFrom(alloc, "standby-a", started.backup_lsn));
    try std.testing.expectError(
        error.SlotSeeding,
        primary.standbyStatusUpdate("standby-a", identity.timeline_id, started.backup_lsn, started.backup_lsn),
    );

    const names = [_][]const u8{"standby-a"};
    const seeding_decision = try primary.evaluateDurability(started.backup_lsn, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(@as(usize, 0), seeding_decision.candidate_count);
    try std.testing.expectEqual(DurabilityStatus.would_block, seeding_decision.status);

    try std.testing.expectError(
        error.SeedBackupNotComplete,
        primary.activateSeededSlot(
            "standby-a",
            identity.timeline_id,
            started.backup_lsn,
            started.backup_lsn,
            started.backup_lsn,
        ),
    );
    try std.testing.expectEqual(slot_store.SlotLifecycle.seeding, (primary.slot("standby-a") orelse return error.TestExpectedEqual).lifecycle);

    var entry = (try primary.log.entryAt(alloc, started.backup_lsn)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(replication_record.RecordKind.backup_start, entry.record.kind);
    try std.testing.expect(std.mem.indexOf(u8, entry.record.payload, "\"manifest_id\":\"manifest-1\"") != null);
}

test "storage.ha primary recovers reservation durable before backup start" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-start-orphan");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "before-backup" });
    try primary.reserveBaseBackupSlot("standby-a", 2, 1);

    const recovered = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectEqual(@as(u64, 2), recovered.backup_lsn);
    try std.testing.expectEqual(@as(u64, 2), recovered.start_record_lsn);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    const retried = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectEqual(recovered.backup_lsn, retried.backup_lsn);
}

test "storage.ha primary advances orphaned reservation after intervening WAL append" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-start-orphan-occupied");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "before-backup" });
    try primary.reserveBaseBackupSlot("standby-a", 2, 1);
    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "intervening-write" }));

    const recovered = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectEqual(@as(u64, 3), recovered.backup_lsn);
    try std.testing.expectEqual(@as(u64, 3), recovered.start_record_lsn);
    try std.testing.expectEqual(@as(u64, 3), primary.lastLsn());

    const retried = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectEqual(recovered.backup_lsn, retried.backup_lsn);
}

test "storage.ha primary retention cap can expire an inactive seeding slot" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "seeding-retention-cap");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "before-backup" });
    _ = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    _ = try primary.append(.{ .payload = "after-backup" });

    const uncapped = try primary.retentionSnapshot(.{});
    try std.testing.expectEqual(@as(usize, 0), uncapped.active_slots);
    try std.testing.expect(uncapped.retained_byte_count > 1);

    const capped = try primary.retentionSnapshot(.{ .max_retained_bytes = 1 });
    try std.testing.expectEqual(@as(u64, 0), capped.retained_lsn_count);
    try std.testing.expectEqual(@as(usize, 1), capped.reseed_recommended);
    try std.testing.expect((primary.slot("standby-a") orelse return error.TestExpectedEqual).reseed_required);
}

test "storage.ha primary marks oldest slots when retained byte cap is exceeded" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "retention-byte-cap");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one-one-one-one-one-one" });
    _ = try primary.append(.{ .payload = "two-two-two-two-two-two" });
    _ = try primary.append(.{ .payload = "three-three-three-three" });
    try primary.createSlot("old", 1);
    try primary.createSlot("current", 3);

    const uncapped = try primary.retentionSnapshot(.{});
    const current_only_bytes = try primary.log.encodedByteCount(3, primary.lastLsn());
    try std.testing.expect(uncapped.retained_byte_count > current_only_bytes);

    const capped = try primary.retentionSnapshot(.{ .max_retained_bytes = current_only_bytes });
    try std.testing.expect(capped.retained_byte_count <= current_only_bytes);
    try std.testing.expectEqual(@as(usize, 1), capped.active_slots);
    try std.testing.expectEqual(@as(usize, 1), capped.reseed_recommended);
    try std.testing.expectEqual(@as(u64, 3), capped.oldest_restart_lsn);

    const old = primary.slot("old") orelse return error.TestExpectedEqual;
    const current = primary.slot("current") orelse return error.TestExpectedEqual;
    try std.testing.expect(old.reseed_required);
    try std.testing.expect(!current.reseed_required);
}

test "storage.ha primary marks oldest slots when retained age cap is exceeded" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "retention-age-cap");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one", .commit_timestamp_ns = 100 });
    _ = try primary.append(.{ .payload = "two", .commit_timestamp_ns = 200 });
    _ = try primary.append(.{ .payload = "three", .commit_timestamp_ns = 500 });
    try primary.createSlot("old", 1);
    try primary.createSlot("current", 3);

    const uncapped = try primary.retentionSnapshot(.{});
    try std.testing.expectEqual(@as(u64, 400), uncapped.retained_age_ns);

    const capped = try primary.retentionSnapshot(.{ .max_retained_age_ns = 50 });
    try std.testing.expect(capped.retained_age_ns <= 50);
    try std.testing.expectEqual(@as(usize, 1), capped.active_slots);
    try std.testing.expectEqual(@as(usize, 1), capped.reseed_recommended);
    try std.testing.expectEqual(@as(u64, 3), capped.oldest_restart_lsn);

    const old = primary.slot("old") orelse return error.TestExpectedEqual;
    const current = primary.slot("current") orelse return error.TestExpectedEqual;
    try std.testing.expect(old.reseed_required);
    try std.testing.expect(!current.reseed_required);
}

test "storage.ha primary rejects base backup over healthy existing slot" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-slot-in-use");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    try std.testing.expectError(error.BaseBackupSlotInUse, primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    }));

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 0), slot.restart_lsn);
    try std.testing.expectEqual(@as(u64, 0), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 0), slot.applied_lsn);
}

test "storage.ha primary allows base backup to reset reseed-required slot" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-reseed");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 2);
    try primary.slots.markReseedRequired("standby-a");

    const started = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectEqual(@as(u64, 3), started.backup_lsn);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expect(!slot.reseed_required);
    try std.testing.expectEqual(@as(u64, 3), slot.restart_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);
}

test "storage.ha primary escapes backup start json payload fields" {
    const alloc = std.testing.allocator;
    const identity = testIdentity();

    const payload = try backupStartPayload(
        alloc,
        identity,
        "standby-\"a\\b\nc",
        "manifest-\"x\\y\nz",
        42,
    );
    defer alloc.free(payload);

    const Payload = struct {
        cluster_id: u64,
        shard_id: u64,
        table_id: u64,
        timeline_id: u64,
        epoch: u64,
        slot_name: []const u8,
        manifest_id: []const u8,
        backup_lsn: u64,
    };
    var parsed = try std.json.parseFromSlice(Payload, alloc, payload, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(identity.cluster_id, parsed.value.cluster_id);
    try std.testing.expectEqual(identity.shard_id, parsed.value.shard_id);
    try std.testing.expectEqual(identity.table_id, parsed.value.table_id);
    try std.testing.expectEqual(identity.timeline_id, parsed.value.timeline_id);
    try std.testing.expectEqual(identity.epoch, parsed.value.epoch);
    try std.testing.expectEqualStrings("standby-\"a\\b\nc", parsed.value.slot_name);
    try std.testing.expectEqualStrings("manifest-\"x\\y\nz", parsed.value.manifest_id);
    try std.testing.expectEqual(@as(u64, 42), parsed.value.backup_lsn);
}

test "storage.ha primary ends base backup with decodable manifest payload" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-end");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    const started = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    _ = try primary.append(.{ .payload = "during-copy" });

    const files = [_]backup_manifest.FileEntry{
        .{ .path = "store/manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "store/sst/0001", .kind = .sstable, .size_bytes = 3, .crc32 = backup_manifest.crc32("sst") },
    };
    const ended = try primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "manifest-1",
        .backup_lsn = started.backup_lsn,
        .checkpoint_lsn = primary.lastLsn(),
        .files = &files,
    });
    try std.testing.expectEqual(started.backup_lsn, ended.backup_lsn);
    try std.testing.expectEqual(@as(u64, 3), ended.end_record_lsn);
    try std.testing.expectEqualStrings("manifest-1", ended.manifest_id);

    // Verification of the restored checkpoint, not backup publication, is the
    // event that makes this slot streamable.
    try std.testing.expectError(
        error.SeedCheckpointMismatch,
        primary.activateSeededSlot("standby-a", identity.timeline_id, 2, 1, 1),
    );
    try primary.activateSeededSlot("standby-a", identity.timeline_id, 2, 2, 2);
    const activated = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(slot_store.SlotLifecycle.streaming, activated.lifecycle);
    try std.testing.expect(activated.active);

    const entries = try primary.streamFrom(alloc, "standby-a", ended.end_record_lsn);
    defer replication_log.freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(replication_record.RecordKind.backup_end, entries[0].record.kind);
    try std.testing.expectEqual(replication_record.PayloadCodec.binary, entries[0].record.payload_codec);

    const decoded = try backup_manifest.decodeAlloc(alloc, entries[0].record.payload);
    defer backup_manifest.freeDecoded(alloc, decoded);
    try std.testing.expectEqual(started.backup_lsn, decoded.backup_lsn);
    try std.testing.expectEqual(@as(u64, 2), decoded.checkpoint_lsn);
    try std.testing.expectEqual(@as(usize, 2), decoded.files.len);
}

test "storage.ha primary rejects backup end from wrong identity or missing start" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-invalid");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    const files = [_]backup_manifest.FileEntry{
        .{ .path = "store/manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
    };
    try std.testing.expectError(error.BackupStartNotDurable, primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "missing",
        .backup_lsn = 5,
        .checkpoint_lsn = 5,
        .files = &files,
    }));

    var wrong_identity = identity;
    wrong_identity.timeline_id = 2;
    try std.testing.expectError(error.WrongTimeline, primary.endBaseBackup(.{
        .identity = wrong_identity,
        .manifest_id = "wrong",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &files,
    }));
}

test "storage.ha primary requires matching backup start record before backup end" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-end-start-match");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    const files = [_]backup_manifest.FileEntry{
        .{ .path = "store/manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
    };

    _ = try primary.append(.{ .payload = "not-a-backup-start" });
    try std.testing.expectError(error.BackupStartNotFound, primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "manifest-1",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &files,
    }));

    const started = try primary.beginBaseBackup(.{
        .slot_name = "standby-a",
        .manifest_id = "manifest-1",
    });
    try std.testing.expectError(error.BackupStartMismatch, primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "manifest-2",
        .backup_lsn = started.backup_lsn,
        .checkpoint_lsn = started.backup_lsn,
        .files = &files,
    }));

    try std.testing.expectError(error.BackupCheckpointNotDurable, primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "manifest-1",
        .backup_lsn = started.backup_lsn,
        .checkpoint_lsn = primary.lastLsn() + 1,
        .files = &files,
    }));
}

test "storage.ha primary requires backup slot retention before backup end" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "backup-end-slot-retention");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    const files = [_]backup_manifest.FileEntry{
        .{ .path = "store/manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
    };

    const dropped = try primary.beginBaseBackup(.{
        .slot_name = "standby-dropped",
        .manifest_id = "manifest-dropped",
    });
    try primary.dropSlot("standby-dropped");
    try std.testing.expectError(error.BackupSlotNotFound, primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "manifest-dropped",
        .backup_lsn = dropped.backup_lsn,
        .checkpoint_lsn = dropped.backup_lsn,
        .files = &files,
    }));

    const paused = try primary.beginBaseBackup(.{
        .slot_name = "standby-paused",
        .manifest_id = "manifest-paused",
    });
    try std.testing.expectError(error.SlotSeeding, primary.pauseSlot("standby-paused"));
    try std.testing.expectError(error.SlotSeeding, primary.resumeSlot("standby-paused"));
    try primary.slots.markReseedRequired("standby-paused");
    try std.testing.expectError(error.BackupSlotNotRetained, primary.endBaseBackup(.{
        .identity = identity,
        .manifest_id = "manifest-paused",
        .backup_lsn = paused.backup_lsn,
        .checkpoint_lsn = paused.backup_lsn,
        .files = &files,
    }));
}

test "storage.ha primary evaluates async remote write and remote apply policies" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "durability");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("a", 0);
    try primary.createSlot("b", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    _ = try primary.append(.{ .payload = "three" });
    try primary.standbyStatusUpdate("a", identity.timeline_id, 3, 1);
    try primary.standbyStatusUpdate("b", identity.timeline_id, 3, 3);

    const names = [_][]const u8{ "a", "b" };
    var decision = try primary.evaluateDurability(3, .{ .mode = .async });
    try std.testing.expectEqual(DurabilityStatus.satisfied, decision.status);

    decision = try primary.evaluateDurability(3, .{
        .mode = .remote_write,
        .selection = .any,
        .required = 2,
        .standby_names = &names,
    });
    try std.testing.expectEqual(DurabilityStatus.satisfied, decision.status);
    try std.testing.expectEqual(@as(usize, 2), decision.satisfied_count);

    decision = try primary.evaluateDurability(3, .{
        .mode = .remote_apply,
        .selection = .any,
        .required = 2,
        .standby_names = &names,
        .failure_policy = .fail_closed,
    });
    try std.testing.expectEqual(DurabilityStatus.fail_closed, decision.status);
    try std.testing.expectEqual(@as(usize, 1), decision.satisfied_count);
    try std.testing.expectEqual(@as(usize, 2), decision.required_count);
}

test "storage.ha primary any policy reports nth standby progress" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "any-progress");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    try primary.createSlot("standby-b", 0);
    try primary.createSlot("standby-c", 0);
    try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));
    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "two" }));
    try std.testing.expectEqual(@as(u64, 3), try primary.append(.{ .payload = "three" }));
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 3, 3);
    try primary.standbyStatusUpdate("standby-b", identity.timeline_id, 2, 2);
    try primary.standbyStatusUpdate("standby-c", identity.timeline_id, 1, 1);

    const names = [_][]const u8{ "standby-a", "standby-b", "standby-c" };
    const decision = try primary.evaluateDurability(3, .{
        .mode = .remote_apply,
        .selection = .any,
        .required = 2,
        .standby_names = &names,
    });
    try std.testing.expectEqual(DurabilityStatus.would_block, decision.status);
    try std.testing.expectEqual(@as(usize, 1), decision.satisfied_count);
    try std.testing.expectEqual(@as(usize, 2), decision.required_count);
    try std.testing.expectEqual(@as(usize, 3), decision.candidate_count);
    try std.testing.expectEqual(@as(u64, 2), decision.progress_lsn);
    try std.testing.expectEqual(@as(u64, 1), decision.missing_lsn_count);
}

test "storage.ha primary first priority policy waits for the first eligible standby" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "first");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("priority-a", 0);
    try primary.createSlot("priority-b", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    try primary.standbyStatusUpdate("priority-a", identity.timeline_id, 2, 1);
    try primary.standbyStatusUpdate("priority-b", identity.timeline_id, 2, 2);

    const names = [_][]const u8{ "priority-a", "priority-b" };
    var decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .selection = .first,
        .required = 1,
        .standby_names = &names,
    });
    try std.testing.expectEqual(DurabilityStatus.would_block, decision.status);
    try std.testing.expectEqual(@as(usize, 1), decision.candidate_count);
    try std.testing.expectEqual(@as(usize, 0), decision.satisfied_count);

    try primary.slots.markReseedRequired("priority-a");
    decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .selection = .first,
        .required = 1,
        .standby_names = &names,
    });
    try std.testing.expectEqual(DurabilityStatus.satisfied, decision.status);
    try std.testing.expectEqual(@as(usize, 1), decision.candidate_count);
    try std.testing.expectEqual(@as(usize, 1), decision.satisfied_count);
}

test "storage.ha primary rejects impossible named synchronous quorum policies" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-sync-quorum");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });

    const names = [_][]const u8{"standby-a"};
    try std.testing.expectError(error.InvalidSyncPolicy, primary.evaluateDurability(1, .{
        .mode = .remote_write,
        .selection = .any,
        .required = 2,
        .standby_names = &names,
    }));
    try std.testing.expectError(error.InvalidSyncPolicy, primary.evaluateDurability(1, .{
        .mode = .remote_apply,
        .selection = .first,
        .required = 2,
        .standby_names = &names,
    }));

    const all_decision = try primary.evaluateDurability(1, .{
        .mode = .remote_apply,
        .selection = .all,
        .required = 2,
        .standby_names = &names,
    });
    try std.testing.expectEqual(@as(usize, 1), all_decision.required_count);
}

test "storage.ha primary refuses streaming from reseed or expired slots" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "retention");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 2);
    try std.testing.expectError(error.WalNoLongerRetained, primary.streamFrom(alloc, "standby-a", 1));

    try primary.slots.markReseedRequired("standby-a");
    try std.testing.expectError(error.SlotRequiresReseed, primary.streamFrom(alloc, "standby-a", 2));
}

test "storage.ha primary rejects replication start beyond next lsn" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "stream-ahead");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });

    const tail = try primary.streamFrom(alloc, "standby-a", primary.nextLsn());
    defer replication_log.freeEntries(alloc, tail);
    try std.testing.expectEqual(@as(usize, 0), tail.len);

    try std.testing.expectError(
        error.ReplicationStartAheadOfPrimary,
        primary.streamFrom(alloc, "standby-a", primary.nextLsn() + 1),
    );
}

test "storage.ha primary scopes retention to the current timeline" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "timeline-retention");
    defer paths.deinit(alloc);
    var identity = testIdentity();
    identity.timeline_id = 2;
    identity.epoch = 2;

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    _ = try primary.append(.{ .payload = "three" });
    try primary.createSlot("current-timeline", 3);
    try primary.slots.createOrUpdate(.{
        .name = "old-timeline",
        .timeline_id = 1,
        .restart_lsn = 1,
        .received_lsn = 1,
        .applied_lsn = 1,
        .safe_read_lsn = 1,
    });

    const retention = try primary.retentionSnapshot(.{});
    try std.testing.expectEqual(@as(usize, 1), retention.active_slots);
    try std.testing.expectEqual(@as(usize, 1), retention.reseed_recommended);
    try std.testing.expectEqual(@as(u64, 3), retention.oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 1), retention.retained_lsn_count);

    const old = primary.slot("old-timeline") orelse return error.TestExpectedEqual;
    try std.testing.expect(old.reseed_required);
    try std.testing.expectError(error.SlotRequiresReseed, primary.streamFrom(alloc, "old-timeline", 1));
}

test "storage.ha primary rejects status updates for old timeline slots" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "timeline-ack");
    defer paths.deinit(alloc);
    var identity = testIdentity();
    identity.timeline_id = 2;
    identity.epoch = 2;

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    try primary.slots.createOrUpdate(.{
        .name = "old-timeline",
        .timeline_id = 1,
        .restart_lsn = 1,
        .received_lsn = 1,
        .applied_lsn = 1,
        .safe_read_lsn = 1,
    });

    try std.testing.expectError(
        error.WrongTimeline,
        primary.standbyStatusUpdate("old-timeline", identity.timeline_id, 1, 1),
    );
    const old = primary.slot("old-timeline") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), old.timeline_id);
    try std.testing.expect(!old.reseed_required);
}

test "storage.ha primary rejects status updates for inactive or reseed slots" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "inactive-ack");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("paused", 0);
    try primary.createSlot("reseed", 0);
    _ = try primary.append(.{ .payload = "one" });

    try primary.pauseSlot("paused");
    try std.testing.expectError(
        error.SlotInactive,
        primary.standbyStatusUpdate("paused", identity.timeline_id, 1, 1),
    );

    try primary.slots.markReseedRequired("reseed");
    try std.testing.expectError(
        error.SlotRequiresReseed,
        primary.standbyStatusUpdate("reseed", identity.timeline_id, 1, 1),
    );

    const paused = primary.slot("paused") orelse return error.TestExpectedEqual;
    try std.testing.expect(!paused.active);
    try std.testing.expectEqual(@as(u64, 0), paused.received_lsn);

    const reseed = primary.slot("reseed") orelse return error.TestExpectedEqual;
    try std.testing.expect(reseed.reseed_required);
    try std.testing.expectEqual(@as(u64, 0), reseed.received_lsn);
}

test "storage.ha primary pauses and resumes slot streaming across reopen" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "pause-resume");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    {
        var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
        defer primary.close();
        try primary.createSlot("standby-a", 0);
        _ = try primary.append(.{ .payload = "one" });
        try primary.pauseSlot("standby-a");
        try std.testing.expectError(error.SlotInactive, primary.streamFrom(alloc, "standby-a", 1));
    }

    {
        var reopened = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
        defer reopened.close();
        const paused = reopened.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(!paused.active);
        try std.testing.expectError(error.SlotInactive, reopened.streamFrom(alloc, "standby-a", 1));
        try reopened.resumeSlot("standby-a");
        const entries = try reopened.streamFrom(alloc, "standby-a", 1);
        defer replication_log.freeEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqualStrings("one", entries[0].record.payload);
    }
}
