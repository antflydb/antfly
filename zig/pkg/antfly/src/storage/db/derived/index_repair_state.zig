// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Replica-local durable state for index reconstruction.
//!
//! This file is deliberately separate from the primary document store: repair
//! candidates and replay pins belong to one physical replica and must not be
//! copied by logical table replication.  Intent and pin changes are rewritten
//! as one checksummed, fsync+rename checkpoint so restart can never observe one
//! without the other.

const std = @import("std");
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../../common/fs_paths.zig");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const types = @import("../types.zig");

const file_name = "index_repair.checkpoint";
const magic = "AFIDXRP1";
const format_version: u32 = 7;
const max_file_bytes: usize = 16 * 1024 * 1024;
const max_entries: usize = 65_536;
const max_index_name_bytes: usize = 4 * 1024;
const max_candidate_path_bytes: usize = 16 * 1024;
const max_build_resume_key_bytes: usize = 64 * 1024;
const max_error_bytes: usize = 16 * 1024;

pub const Trigger = enum(u8) {
    incomplete_bulk_publish = 1,
    operator_generation_rebuild = 2,
    /// A physical/root identity changed while a pointer-selected candidate was
    /// still protected by an unfinished intent. The old candidate is never
    /// resumed under the new identity; a fresh generation is rebuilt while
    /// the affected index remains unavailable.
    root_generation_rebuild = 3,
    /// The serving dense generation does not have the same artifact cardinality
    /// as the authoritative source counter. It remains unavailable until a
    /// replacement is reconstructed and validated.
    artifact_coverage_mismatch = 4,
    /// The authoritative artifact counter is absent. The serving generation
    /// remains unavailable while the counter is bootstrapped and a replacement
    /// is validated.
    artifact_counter_missing = 5,
    /// Configuration/checkpoint state or backend validation proves that the
    /// current generation is unsafe to serve. Repair remains fail-closed until
    /// a replacement reaches cleanup.
    projection_generation_invalid = 6,
    /// An operator requested an online generation rebuild, but BackendRuntime
    /// has not yet proved that the currently serving dense generation is safe.
    /// This state is fail-closed and may be promoted to the online operator
    /// rebuild trigger only after generation-scoped background validation.
    operator_generation_validation = 7,
};

pub const Phase = enum(u8) {
    detected = 1,
    preflight = 2,
    building = 3,
    catching_up = 4,
    ready = 5,
    waiting_for_convergence = 6,
    activating = 7,
    validating = 8,
    cleanup = 9,
    terminal = 10,
};

pub const Automation = enum(u8) {
    enabled = 1,
    paused = 2,
};

pub const ReplicaIdentity = struct {
    db_identity: u128,
    replica_id: u128,
    root_generation: u64,

    pub fn eql(a: @This(), b: @This()) bool {
        return a.db_identity == b.db_identity and
            a.replica_id == b.replica_id and
            a.root_generation == b.root_generation;
    }
};

pub const IndexRepairIntent = struct {
    version: u8 = 1,
    repair_id: u128,
    /// Monotonic checkpoint-local compare-and-swap revision. Unlike phase,
    /// every mutation advances this value, including pause, retry, pin, and
    /// progress-only updates.
    revision: u64 = 0,
    db_identity: u128,
    group_id: u64,
    replica_id: u128,
    root_generation: u64,
    index_name: []u8,
    kind: types.IndexKind,
    config_hash: u64,
    trigger: Trigger = .incomplete_bulk_publish,
    /// Stable API job identity for crash-idempotent forced generation rebuilds.
    /// Both values are zero until an operator job attaches to the intent.
    operator_job_id: u64 = 0,
    operator_job_created_at_ms: u64 = 0,
    candidate_relative_path: ?[]u8 = null,
    /// Durable rollback anchor captured immediately before activation. A
    /// captured null pointer means the canonical index root was active.
    previous_pointer_captured: bool = false,
    previous_active_relative_path: ?[]u8 = null,
    detected_sequence: u64,
    build_floor_sequence: u64 = 0,
    /// Last source-store key durably incorporated into either admission's
    /// generated-enrichment replay (`detected`) or a reopenable building
    /// candidate (`building`). Resume scans begin strictly after this key. The
    /// cumulative count is diagnostic/accounting state, not correctness state.
    build_resume_key: ?[]u8 = null,
    build_reprocessed: u64 = 0,
    candidate_applied_sequence: u64 = 0,
    /// Reconstructible node plan, not a durable reservation. The estimate is
    /// candidate bytes; planned bytes additionally include replay/WAL growth
    /// and cleanup headroom. Restart always obtains a new process-local claim.
    estimated_candidate_bytes: u64 = 0,
    planned_disk_bytes: u64 = 0,
    target_sequence: u64,
    phase: Phase = .detected,
    attempt_count: u32 = 0,
    /// Consecutive failed/deferred attempts used for retry backoff. Successful
    /// cooperative progress resets this without destroying the lifetime
    /// attempt counter exposed for diagnostics.
    failure_streak: u32 = 0,
    next_retry_at_ms: u64 = 0,
    started_at_ms: u64,
    updated_at_ms: u64,
    owner_epoch: u64,
    automation: Automation = .enabled,
    last_error: ?[]u8 = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        if (self.candidate_relative_path) |value| alloc.free(value);
        if (self.build_resume_key) |value| alloc.free(value);
        if (self.previous_active_relative_path) |value| alloc.free(value);
        if (self.last_error) |value| alloc.free(value);
        self.* = undefined;
    }

    pub fn clone(self: @This(), alloc: Allocator) !@This() {
        const index_name = try alloc.dupe(u8, self.index_name);
        errdefer alloc.free(index_name);
        const candidate = if (self.candidate_relative_path) |value| try alloc.dupe(u8, value) else null;
        errdefer if (candidate) |value| alloc.free(value);
        const previous_active = if (self.previous_active_relative_path) |value| try alloc.dupe(u8, value) else null;
        errdefer if (previous_active) |value| alloc.free(value);
        const build_resume_key = if (self.build_resume_key) |value| try alloc.dupe(u8, value) else null;
        errdefer if (build_resume_key) |value| alloc.free(value);
        const last_error = if (self.last_error) |value| try alloc.dupe(u8, value) else null;
        errdefer if (last_error) |value| alloc.free(value);
        var out = self;
        out.index_name = index_name;
        out.candidate_relative_path = candidate;
        out.previous_active_relative_path = previous_active;
        out.build_resume_key = build_resume_key;
        out.last_error = last_error;
        return out;
    }

    pub fn identity(self: @This()) ReplicaIdentity {
        return .{
            .db_identity = self.db_identity,
            .replica_id = self.replica_id,
            .root_generation = self.root_generation,
        };
    }
};

pub const IndexRepairReplayPin = struct {
    version: u8 = 1,
    repair_id: u128,
    db_identity: u128,
    replica_id: u128,
    root_generation: u64,
    index_name: []u8,
    retain_after_sequence: u64,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        self.* = undefined;
    }

    pub fn clone(self: @This(), alloc: Allocator) !@This() {
        var out = self;
        out.index_name = try alloc.dupe(u8, self.index_name);
        return out;
    }
};

pub const Entry = struct {
    intent: IndexRepairIntent,
    pin: ?IndexRepairReplayPin = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.intent.deinit(alloc);
        if (self.pin) |*pin| pin.deinit(alloc);
        self.* = undefined;
    }

    pub fn clone(self: @This(), alloc: Allocator) !@This() {
        var intent = try self.intent.clone(alloc);
        errdefer intent.deinit(alloc);
        return .{
            .intent = intent,
            .pin = if (self.pin) |pin| try pin.clone(alloc) else null,
        };
    }
};

pub const State = struct {
    identity: ReplicaIdentity,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.entries.items) |*entry| entry.deinit(alloc);
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    pub fn findIndex(self: *const @This(), index_name: []const u8) ?usize {
        for (self.entries.items, 0..) |entry, i| {
            if (std.mem.eql(u8, entry.intent.index_name, index_name)) return i;
        }
        return null;
    }

    pub fn minimumRetainAfterSequence(self: *const @This()) ?u64 {
        var minimum: ?u64 = null;
        for (self.entries.items) |entry| {
            const pin = entry.pin orelse continue;
            // Zero is the conservative provisional pin: retain everything.
            if (pin.retain_after_sequence == 0) return 0;
            minimum = if (minimum) |value| @min(value, pin.retain_after_sequence) else pin.retain_after_sequence;
        }
        return minimum;
    }
};

pub const ExpectedTransition = struct {
    repair_id: u128,
    revision: u64,
    phase: Phase,
    config_hash: u64,
    root_generation: u64,
    owner_epoch: u64,
};

const checkpoint_alloc = std.heap.page_allocator;
var checkpoint_registry_mutex: std.atomic.Mutex = .unlocked;
var checkpoint_locks: std.StringHashMapUnmanaged(*CheckpointLock) = .empty;

const CheckpointLock = struct {
    path: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    refs: usize = 0,
};

const Guard = struct {
    lock: *CheckpointLock,

    fn release(self: *@This()) void {
        self.lock.mutex.unlock();
        platform_sync.lockYielding(&checkpoint_registry_mutex);
        defer checkpoint_registry_mutex.unlock();
        std.debug.assert(self.lock.refs > 0);
        self.lock.refs -= 1;
        if (self.lock.refs != 0) return;
        const removed = checkpoint_locks.fetchRemove(self.lock.path) orelse unreachable;
        std.debug.assert(removed.value == self.lock);
        checkpoint_alloc.free(self.lock.path);
        checkpoint_alloc.destroy(self.lock);
    }
};

fn acquire(path: []const u8) !Guard {
    platform_sync.lockYielding(&checkpoint_registry_mutex);
    const gop = checkpoint_locks.getOrPut(checkpoint_alloc, path) catch |err| {
        checkpoint_registry_mutex.unlock();
        return err;
    };
    if (!gop.found_existing) {
        const owned_path = checkpoint_alloc.dupe(u8, path) catch |err| {
            _ = checkpoint_locks.remove(path);
            checkpoint_registry_mutex.unlock();
            return err;
        };
        const lock = checkpoint_alloc.create(CheckpointLock) catch |err| {
            checkpoint_alloc.free(owned_path);
            _ = checkpoint_locks.remove(path);
            checkpoint_registry_mutex.unlock();
            return err;
        };
        lock.* = .{ .path = owned_path };
        gop.key_ptr.* = owned_path;
        gop.value_ptr.* = lock;
    }
    const lock = gop.value_ptr.*;
    lock.refs += 1;
    checkpoint_registry_mutex.unlock();
    platform_sync.lockYielding(&lock.mutex);
    return .{ .lock = lock };
}

pub fn checkpointPathAlloc(alloc: Allocator, db_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db_path, file_name });
}

pub fn newReplicaIdentity(alloc: Allocator, root_generation: u64) !ReplicaIdentity {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var entropy: [32]u8 = undefined;
    try io_impl.io().randomSecure(&entropy);
    var db_identity = std.mem.readInt(u128, entropy[0..16], .little);
    var replica_id = std.mem.readInt(u128, entropy[16..32], .little);
    if (db_identity == 0) db_identity = 1;
    if (replica_id == 0) replica_id = 1;
    return .{
        .db_identity = db_identity,
        .replica_id = replica_id,
        .root_generation = root_generation,
    };
}

pub fn newRepairId(alloc: Allocator) !u128 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var entropy: [16]u8 = undefined;
    try io_impl.io().randomSecure(&entropy);
    const value = std.mem.readInt(u128, &entropy, .little);
    return if (value == 0) 1 else value;
}

pub fn loadOrCreate(alloc: Allocator, path: []const u8, root_generation: u64) !State {
    var guard = try acquire(path);
    defer guard.release();
    return loadUnlocked(alloc, path) catch |err| switch (err) {
        error.FileNotFound => blk: {
            var state = State{ .identity = try newReplicaIdentity(alloc, root_generation) };
            errdefer state.deinit(alloc);
            try writeUnlocked(alloc, path, &state);
            break :blk state;
        },
        else => return err,
    };
}

pub fn load(alloc: Allocator, path: []const u8) !State {
    var guard = try acquire(path);
    defer guard.release();
    return try loadUnlocked(alloc, path);
}

/// Rebinds an empty replica-local repair checkpoint to a new physical root
/// generation after the caller has fenced execution and safely discarded all
/// inactive candidates referenced by the old state.
pub fn resetForRootGeneration(
    alloc: Allocator,
    path: []const u8,
    expected_identity: ReplicaIdentity,
    root_generation: u64,
) !State {
    return try resetForRootGenerationWithIntents(alloc, path, expected_identity, root_generation, &.{});
}

/// Atomically replace a retired physical-root identity and bind all supplied
/// reconstruction debts to the new identity. Callers pass detected intents;
/// this function owns identity rebinding so an old candidate can never be
/// resumed under, or accidentally published into, the replacement root.
pub fn resetForRootGenerationWithIntents(
    alloc: Allocator,
    path: []const u8,
    expected_identity: ReplicaIdentity,
    root_generation: u64,
    intents: []const IndexRepairIntent,
) !State {
    var guard = try acquire(path);
    defer guard.release();
    var old = try loadUnlocked(alloc, path);
    defer old.deinit(alloc);
    if (!old.identity.eql(expected_identity)) return error.ReplicaIdentityMismatch;
    var replacement = State{ .identity = try newReplicaIdentity(alloc, root_generation) };
    errdefer replacement.deinit(alloc);
    if (intents.len > max_entries) return error.IndexRepairStateTooLarge;
    for (intents) |intent| {
        var owned = try intent.clone(alloc);
        var owned_transferred = false;
        errdefer if (!owned_transferred) owned.deinit(alloc);
        owned.db_identity = replacement.identity.db_identity;
        owned.replica_id = replacement.identity.replica_id;
        owned.root_generation = replacement.identity.root_generation;
        owned.revision = 1;
        if (owned.phase != .detected or replacement.findIndex(owned.index_name) != null) {
            return error.InvalidIndexRepairTransition;
        }
        try validateEntry(.{ .intent = owned });
        try replacement.entries.append(alloc, .{ .intent = owned });
        owned_transferred = true;
    }
    try writeUnlocked(alloc, path, &replacement);
    return replacement;
}

pub fn putEntry(
    alloc: Allocator,
    path: []const u8,
    expected_identity: ReplicaIdentity,
    expected: ?ExpectedTransition,
    entry: Entry,
) !void {
    try validateEntry(entry);
    if (!entry.intent.identity().eql(expected_identity)) return error.ReplicaIdentityMismatch;

    var guard = try acquire(path);
    defer guard.release();
    var state = try loadUnlocked(alloc, path);
    defer state.deinit(alloc);
    if (!state.identity.eql(expected_identity)) return error.ReplicaIdentityMismatch;

    const existing_index = state.findIndex(entry.intent.index_name);
    if (expected) |transition| {
        const existing = if (existing_index) |i| state.entries.items[i].intent else return error.RepairTransitionConflict;
        if (existing.repair_id != transition.repair_id or
            existing.revision != transition.revision or
            existing.phase != transition.phase or
            existing.config_hash != transition.config_hash or
            existing.root_generation != transition.root_generation or
            existing.owner_epoch != transition.owner_epoch)
        {
            return error.RepairTransitionConflict;
        }
        if (!phaseTransitionAllowed(existing.phase, entry.intent.phase)) {
            return error.InvalidIndexRepairTransition;
        }
    } else {
        if (existing_index != null) return error.RepairTransitionConflict;
        if (entry.intent.phase != .detected or entry.pin != null) {
            return error.InvalidIndexRepairTransition;
        }
    }

    var owned = try entry.clone(alloc);
    var owned_transferred = false;
    errdefer if (!owned_transferred) owned.deinit(alloc);
    owned.intent.revision = if (expected) |transition|
        std.math.add(u64, transition.revision, 1) catch return error.InvalidIndexRepairState
    else
        1;
    if (existing_index) |i| {
        state.entries.items[i].deinit(alloc);
        state.entries.items[i] = owned;
        owned_transferred = true;
    } else {
        if (state.entries.items.len >= max_entries) return error.IndexRepairStateTooLarge;
        try state.entries.append(alloc, owned);
        owned_transferred = true;
    }
    try writeUnlocked(alloc, path, &state);
}

fn phaseTransitionAllowed(from: Phase, to: Phase) bool {
    if (from == to) return true;
    if (to == .terminal) return from != .cleanup;
    return switch (from) {
        .detected => to == .preflight,
        .preflight => to == .building,
        .building => to == .catching_up or to == .preflight,
        .catching_up => to == .ready or to == .waiting_for_convergence or to == .preflight,
        .ready => to == .waiting_for_convergence or to == .catching_up or to == .activating or to == .preflight,
        .waiting_for_convergence => to == .catching_up or to == .ready or to == .activating or to == .preflight,
        .activating => to == .validating or to == .cleanup or to == .waiting_for_convergence or to == .preflight,
        .validating => to == .cleanup or to == .preflight,
        .cleanup => to == .preflight,
        .terminal => to == .detected,
    };
}

pub fn removeEntryAndPin(
    alloc: Allocator,
    path: []const u8,
    expected_identity: ReplicaIdentity,
    expected: ExpectedTransition,
) !void {
    var guard = try acquire(path);
    defer guard.release();
    var state = try loadUnlocked(alloc, path);
    defer state.deinit(alloc);
    if (!state.identity.eql(expected_identity)) return error.ReplicaIdentityMismatch;
    const i = findIndexByRepairId(&state, expected.repair_id) orelse return error.RepairTransitionConflict;
    const intent = state.entries.items[i].intent;
    if (intent.phase != expected.phase or
        intent.revision != expected.revision or
        intent.config_hash != expected.config_hash or
        intent.root_generation != expected.root_generation or
        intent.owner_epoch != expected.owner_epoch)
    {
        return error.RepairTransitionConflict;
    }
    var removed = state.entries.orderedRemove(i);
    defer removed.deinit(alloc);
    try writeUnlocked(alloc, path, &state);
}

fn findIndexByRepairId(state: *const State, repair_id: u128) ?usize {
    for (state.entries.items, 0..) |entry, i| {
        if (entry.intent.repair_id == repair_id) return i;
    }
    return null;
}

fn validateEntry(entry: Entry) !void {
    const intent = entry.intent;
    if (intent.version != 1 or intent.repair_id == 0 or intent.db_identity == 0 or intent.replica_id == 0) return error.InvalidIndexRepairState;
    if (intent.index_name.len == 0 or intent.index_name.len > max_index_name_bytes) return error.InvalidIndexRepairState;
    if (intent.candidate_relative_path) |path| try validateCandidateRelativePath(intent.index_name, path);
    if (intent.previous_active_relative_path) |path| {
        if (!intent.previous_pointer_captured) return error.InvalidIndexRepairState;
        try validateCandidateRelativePath(intent.index_name, path);
    }
    if (intent.last_error) |value| if (value.len > max_error_bytes) return error.InvalidIndexRepairState;
    if (intent.build_resume_key) |value| {
        const admission_replay_cursor = intent.trigger == .incomplete_bulk_publish and
            intent.phase == .detected;
        if (value.len == 0 or value.len > max_build_resume_key_bytes or
            (intent.candidate_relative_path == null and !admission_replay_cursor))
        {
            return error.InvalidIndexRepairState;
        }
    }
    if ((intent.operator_job_id == 0) != (intent.operator_job_created_at_ms == 0)) return error.InvalidIndexRepairState;
    if (intent.planned_disk_bytes != 0 and intent.planned_disk_bytes < intent.estimated_candidate_bytes) return error.InvalidIndexRepairState;
    if (entry.pin) |pin| {
        if (pin.version != 1 or pin.repair_id != intent.repair_id or
            pin.db_identity != intent.db_identity or pin.replica_id != intent.replica_id or
            pin.root_generation != intent.root_generation or
            !std.mem.eql(u8, pin.index_name, intent.index_name))
        {
            return error.InvalidIndexRepairState;
        }
    }
}

pub fn validateCandidateRelativePath(index_name: []const u8, path: []const u8) !void {
    if (path.len == 0 or path.len > max_candidate_path_bytes or std.fs.path.isAbsolute(path)) return error.InvalidRepairCandidatePath;
    var components = std.mem.splitScalar(u8, path, '/');
    const root = components.next() orelse return error.InvalidRepairCandidatePath;
    if (!std.mem.startsWith(u8, root, ".repair-shadow-") or root.len == ".repair-shadow-".len) return error.InvalidRepairCandidatePath;
    const indexes = components.next() orelse return error.InvalidRepairCandidatePath;
    const name = components.next() orelse return error.InvalidRepairCandidatePath;
    if (!std.mem.eql(u8, indexes, "indexes") or !std.mem.eql(u8, name, index_name) or components.next() != null) return error.InvalidRepairCandidatePath;
    if (std.mem.eql(u8, root, ".") or std.mem.eql(u8, root, "..") or
        std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) return error.InvalidRepairCandidatePath;
}

fn loadUnlocked(alloc: Allocator, path: []const u8) !State {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_file_bytes));
    defer alloc.free(raw);
    return try decode(alloc, raw);
}

fn writeUnlocked(alloc: Allocator, path: []const u8, state: *const State) !void {
    const encoded = try encode(alloc, state);
    defer alloc.free(encoded);
    if (std.fs.path.dirname(path)) |parent| {
        var io_parent = std.Io.Threaded.init(alloc, .{});
        defer io_parent.deinit();
        try fs_paths.createDirPathPortable(io_parent.io(), parent);
    }
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{ path, platform_time.monotonicNs() });
    defer alloc.free(tmp_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(encoded);
        try writer.end();
        try file.sync(io);
    }
    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

fn encode(alloc: Allocator, state: *const State) ![]u8 {
    if (state.entries.items.len > max_entries) return error.IndexRepairStateTooLarge;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, magic);
    try appendInt(alloc, &out, u32, format_version);
    try appendInt(alloc, &out, u128, state.identity.db_identity);
    try appendInt(alloc, &out, u128, state.identity.replica_id);
    try appendInt(alloc, &out, u64, state.identity.root_generation);
    try appendInt(alloc, &out, u32, @intCast(state.entries.items.len));
    for (state.entries.items) |entry| {
        try validateEntry(entry);
        const intent = entry.intent;
        try appendInt(alloc, &out, u8, intent.version);
        try appendInt(alloc, &out, u128, intent.repair_id);
        try appendInt(alloc, &out, u64, intent.revision);
        try appendInt(alloc, &out, u128, intent.db_identity);
        try appendInt(alloc, &out, u64, intent.group_id);
        try appendInt(alloc, &out, u128, intent.replica_id);
        try appendInt(alloc, &out, u64, intent.root_generation);
        try appendString(alloc, &out, intent.index_name, max_index_name_bytes);
        try appendInt(alloc, &out, u8, @intFromEnum(intent.kind));
        try appendInt(alloc, &out, u64, intent.config_hash);
        try appendInt(alloc, &out, u8, @intFromEnum(intent.trigger));
        try appendInt(alloc, &out, u64, intent.operator_job_id);
        try appendInt(alloc, &out, u64, intent.operator_job_created_at_ms);
        try appendOptionalString(alloc, &out, intent.candidate_relative_path, max_candidate_path_bytes);
        try appendInt(alloc, &out, u8, @intFromBool(intent.previous_pointer_captured));
        try appendOptionalString(alloc, &out, intent.previous_active_relative_path, max_candidate_path_bytes);
        try appendInt(alloc, &out, u64, intent.detected_sequence);
        try appendInt(alloc, &out, u64, intent.build_floor_sequence);
        try appendOptionalString(alloc, &out, intent.build_resume_key, max_build_resume_key_bytes);
        try appendInt(alloc, &out, u64, intent.build_reprocessed);
        try appendInt(alloc, &out, u64, intent.candidate_applied_sequence);
        try appendInt(alloc, &out, u64, intent.estimated_candidate_bytes);
        // Format versions 2 and 3 called this value "reserved". Its on-disk
        // position is unchanged; only the in-memory name now reflects that a
        // process-local reservation cannot survive restart.
        try appendInt(alloc, &out, u64, intent.planned_disk_bytes);
        try appendInt(alloc, &out, u64, intent.target_sequence);
        try appendInt(alloc, &out, u8, @intFromEnum(intent.phase));
        try appendInt(alloc, &out, u32, intent.attempt_count);
        try appendInt(alloc, &out, u32, intent.failure_streak);
        try appendInt(alloc, &out, u64, intent.next_retry_at_ms);
        try appendInt(alloc, &out, u64, intent.started_at_ms);
        try appendInt(alloc, &out, u64, intent.updated_at_ms);
        try appendInt(alloc, &out, u64, intent.owner_epoch);
        try appendInt(alloc, &out, u8, @intFromEnum(intent.automation));
        try appendOptionalString(alloc, &out, intent.last_error, max_error_bytes);
        try appendInt(alloc, &out, u8, if (entry.pin != null) 1 else 0);
        if (entry.pin) |pin| {
            try appendInt(alloc, &out, u8, pin.version);
            try appendInt(alloc, &out, u128, pin.repair_id);
            try appendInt(alloc, &out, u128, pin.db_identity);
            try appendInt(alloc, &out, u128, pin.replica_id);
            try appendInt(alloc, &out, u64, pin.root_generation);
            try appendString(alloc, &out, pin.index_name, max_index_name_bytes);
            try appendInt(alloc, &out, u64, pin.retain_after_sequence);
        }
    }
    try appendInt(alloc, &out, u32, std.hash.Crc32.hash(out.items));
    if (out.items.len > max_file_bytes) return error.IndexRepairStateTooLarge;
    return try out.toOwnedSlice(alloc);
}

fn decode(alloc: Allocator, raw: []const u8) !State {
    if (raw.len < magic.len + 4 + 4 or !std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidIndexRepairState;
    const payload_end = raw.len - 4;
    const expected_crc = std.mem.readInt(u32, raw[payload_end..][0..4], .little);
    if (std.hash.Crc32.hash(raw[0..payload_end]) != expected_crc) return error.InvalidIndexRepairState;
    var pos: usize = magic.len;
    const decoded_format_version = try readInt(raw[0..payload_end], &pos, u32);
    if (decoded_format_version < 1 or decoded_format_version > format_version) return error.InvalidIndexRepairState;
    var state = State{ .identity = .{
        .db_identity = try readInt(raw[0..payload_end], &pos, u128),
        .replica_id = try readInt(raw[0..payload_end], &pos, u128),
        .root_generation = try readInt(raw[0..payload_end], &pos, u64),
    } };
    errdefer state.deinit(alloc);
    if (state.identity.db_identity == 0 or state.identity.replica_id == 0) return error.InvalidIndexRepairState;
    const count = try readInt(raw[0..payload_end], &pos, u32);
    if (count > max_entries) return error.InvalidIndexRepairState;
    for (0..count) |_| {
        var intent = IndexRepairIntent{
            .version = try readInt(raw[0..payload_end], &pos, u8),
            .repair_id = try readInt(raw[0..payload_end], &pos, u128),
            .revision = if (decoded_format_version >= 4) try readInt(raw[0..payload_end], &pos, u64) else 0,
            .db_identity = try readInt(raw[0..payload_end], &pos, u128),
            .group_id = try readInt(raw[0..payload_end], &pos, u64),
            .replica_id = try readInt(raw[0..payload_end], &pos, u128),
            .root_generation = try readInt(raw[0..payload_end], &pos, u64),
            .index_name = try readString(alloc, raw[0..payload_end], &pos, max_index_name_bytes),
            .kind = try readEnum(types.IndexKind, raw[0..payload_end], &pos),
            .config_hash = try readInt(raw[0..payload_end], &pos, u64),
            .trigger = try readEnum(Trigger, raw[0..payload_end], &pos),
            .operator_job_id = if (decoded_format_version >= 5) try readInt(raw[0..payload_end], &pos, u64) else 0,
            .operator_job_created_at_ms = if (decoded_format_version >= 5) try readInt(raw[0..payload_end], &pos, u64) else 0,
            .candidate_relative_path = null,
            .detected_sequence = 0,
            .target_sequence = 0,
            .started_at_ms = 0,
            .updated_at_ms = 0,
            .owner_epoch = 0,
        };
        errdefer intent.deinit(alloc);
        intent.candidate_relative_path = try readOptionalString(alloc, raw[0..payload_end], &pos, max_candidate_path_bytes);
        if (decoded_format_version >= 3) {
            const previous_pointer_captured = try readInt(raw[0..payload_end], &pos, u8);
            if (previous_pointer_captured > 1) return error.InvalidIndexRepairState;
            intent.previous_pointer_captured = previous_pointer_captured == 1;
            intent.previous_active_relative_path = try readOptionalString(alloc, raw[0..payload_end], &pos, max_candidate_path_bytes);
        }
        intent.detected_sequence = try readInt(raw[0..payload_end], &pos, u64);
        intent.build_floor_sequence = try readInt(raw[0..payload_end], &pos, u64);
        if (decoded_format_version >= 6) {
            intent.build_resume_key = try readOptionalString(alloc, raw[0..payload_end], &pos, max_build_resume_key_bytes);
            intent.build_reprocessed = try readInt(raw[0..payload_end], &pos, u64);
        }
        intent.candidate_applied_sequence = try readInt(raw[0..payload_end], &pos, u64);
        if (decoded_format_version >= 2) {
            intent.estimated_candidate_bytes = try readInt(raw[0..payload_end], &pos, u64);
            intent.planned_disk_bytes = try readInt(raw[0..payload_end], &pos, u64);
        }
        intent.target_sequence = try readInt(raw[0..payload_end], &pos, u64);
        intent.phase = try readEnum(Phase, raw[0..payload_end], &pos);
        intent.attempt_count = try readInt(raw[0..payload_end], &pos, u32);
        if (decoded_format_version >= 6) {
            intent.failure_streak = try readInt(raw[0..payload_end], &pos, u32);
        }
        intent.next_retry_at_ms = try readInt(raw[0..payload_end], &pos, u64);
        intent.started_at_ms = try readInt(raw[0..payload_end], &pos, u64);
        intent.updated_at_ms = try readInt(raw[0..payload_end], &pos, u64);
        intent.owner_epoch = try readInt(raw[0..payload_end], &pos, u64);
        intent.automation = try readEnum(Automation, raw[0..payload_end], &pos);
        intent.last_error = try readOptionalString(alloc, raw[0..payload_end], &pos, max_error_bytes);
        const has_pin = try readInt(raw[0..payload_end], &pos, u8);
        if (has_pin > 1) return error.InvalidIndexRepairState;
        var pin: ?IndexRepairReplayPin = null;
        if (has_pin == 1) {
            pin = .{
                .version = try readInt(raw[0..payload_end], &pos, u8),
                .repair_id = try readInt(raw[0..payload_end], &pos, u128),
                .db_identity = try readInt(raw[0..payload_end], &pos, u128),
                .replica_id = try readInt(raw[0..payload_end], &pos, u128),
                .root_generation = try readInt(raw[0..payload_end], &pos, u64),
                .index_name = try readString(alloc, raw[0..payload_end], &pos, max_index_name_bytes),
                .retain_after_sequence = try readInt(raw[0..payload_end], &pos, u64),
            };
        }
        errdefer if (pin) |*value| value.deinit(alloc);
        const entry = Entry{ .intent = intent, .pin = pin };
        try validateEntry(entry);
        if (!intent.identity().eql(state.identity) or state.findIndex(intent.index_name) != null) return error.InvalidIndexRepairState;
        try state.entries.append(alloc, entry);
    }
    if (pos != payload_end) return error.InvalidIndexRepairState;
    return state;
}

fn appendInt(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), comptime T: type, value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn readInt(raw: []const u8, pos: *usize, comptime T: type) !T {
    if (pos.* + @sizeOf(T) > raw.len) return error.InvalidIndexRepairState;
    const value = std.mem.readInt(T, raw[pos.*..][0..@sizeOf(T)], .little);
    pos.* += @sizeOf(T);
    return value;
}

fn readEnum(comptime T: type, raw: []const u8, pos: *usize) !T {
    const value = try readInt(raw, pos, u8);
    inline for (@typeInfo(T).@"enum".fields) |field| {
        if (field.value == value) return @enumFromInt(value);
    }
    return error.InvalidIndexRepairState;
}

fn appendString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8, maximum: usize) !void {
    if (value.len > maximum) return error.InvalidIndexRepairState;
    try appendInt(alloc, out, u32, @intCast(value.len));
    try out.appendSlice(alloc, value);
}

fn readString(alloc: Allocator, raw: []const u8, pos: *usize, maximum: usize) ![]u8 {
    const len = try readInt(raw, pos, u32);
    if (len > maximum or pos.* + len > raw.len) return error.InvalidIndexRepairState;
    const value = try alloc.dupe(u8, raw[pos.* .. pos.* + len]);
    pos.* += len;
    return value;
}

fn appendOptionalString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: ?[]const u8, maximum: usize) !void {
    try appendInt(alloc, out, u8, if (value != null) 1 else 0);
    if (value) |bytes| try appendString(alloc, out, bytes, maximum);
}

fn readOptionalString(alloc: Allocator, raw: []const u8, pos: *usize, maximum: usize) !?[]u8 {
    const present = try readInt(raw, pos, u8);
    if (present > 1) return error.InvalidIndexRepairState;
    return if (present == 1) try readString(alloc, raw, pos, maximum) else null;
}

fn testEntry(alloc: Allocator, identity: ReplicaIdentity, phase: Phase) !Entry {
    return .{
        .intent = .{
            .repair_id = 91,
            .db_identity = identity.db_identity,
            .group_id = 7,
            .replica_id = identity.replica_id,
            .root_generation = identity.root_generation,
            .index_name = try alloc.dupe(u8, "dense_idx"),
            .kind = .dense_vector,
            .config_hash = 44,
            .candidate_relative_path = if (phase == .detected) null else try alloc.dupe(u8, ".repair-shadow-91/indexes/dense_idx"),
            .detected_sequence = 10,
            .target_sequence = 12,
            .phase = phase,
            .started_at_ms = 100,
            .updated_at_ms = 101,
            .owner_epoch = 3,
        },
        .pin = if (phase == .detected) null else .{
            .repair_id = 91,
            .db_identity = identity.db_identity,
            .replica_id = identity.replica_id,
            .root_generation = identity.root_generation,
            .index_name = try alloc.dupe(u8, "dense_idx"),
            .retain_after_sequence = 0,
        },
    };
}

test "index repair state persists intent and provisional replay pin atomically" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path = try checkpointPathAlloc(alloc, root);
    defer alloc.free(path);

    var initial = try loadOrCreate(alloc, path, 8);
    const identity = initial.identity;
    initial.deinit(alloc);
    var detected = try testEntry(alloc, identity, .detected);
    defer detected.deinit(alloc);
    try putEntry(alloc, path, identity, null, detected);
    var preflight = try testEntry(alloc, identity, .preflight);
    defer preflight.deinit(alloc);
    try putEntry(alloc, path, identity, .{
        .repair_id = 91,
        .revision = 1,
        .phase = .detected,
        .config_hash = 44,
        .root_generation = 8,
        .owner_epoch = 3,
    }, preflight);
    var entry = try testEntry(alloc, identity, .building);
    defer entry.deinit(alloc);
    entry.intent.build_floor_sequence = 11;
    entry.intent.build_resume_key = try alloc.dupe(u8, "artifact-key:42");
    entry.intent.build_reprocessed = 42;
    entry.intent.failure_streak = 3;
    entry.intent.trigger = .projection_generation_invalid;
    entry.intent.operator_job_id = 77;
    entry.intent.operator_job_created_at_ms = 1234;
    entry.intent.previous_pointer_captured = true;
    entry.intent.previous_active_relative_path = try alloc.dupe(
        u8,
        ".repair-shadow-77/indexes/dense_idx",
    );
    try putEntry(alloc, path, identity, .{
        .repair_id = 91,
        .revision = 2,
        .phase = .preflight,
        .config_hash = 44,
        .root_generation = 8,
        .owner_epoch = 3,
    }, entry);

    var reopened = try load(alloc, path);
    defer reopened.deinit(alloc);
    try std.testing.expect(reopened.identity.eql(identity));
    try std.testing.expectEqual(@as(usize, 1), reopened.entries.items.len);
    try std.testing.expectEqual(@as(?u64, 0), reopened.minimumRetainAfterSequence());
    try std.testing.expectEqual(Phase.building, reopened.entries.items[0].intent.phase);
    try std.testing.expectEqualStrings("artifact-key:42", reopened.entries.items[0].intent.build_resume_key.?);
    try std.testing.expectEqual(@as(u64, 42), reopened.entries.items[0].intent.build_reprocessed);
    try std.testing.expectEqual(@as(u32, 3), reopened.entries.items[0].intent.failure_streak);
    try std.testing.expectEqual(Trigger.projection_generation_invalid, reopened.entries.items[0].intent.trigger);
    try std.testing.expectEqual(@as(u64, 77), reopened.entries.items[0].intent.operator_job_id);
    try std.testing.expectEqual(@as(u64, 1234), reopened.entries.items[0].intent.operator_job_created_at_ms);
    try std.testing.expect(reopened.entries.items[0].intent.previous_pointer_captured);
    try std.testing.expectEqualStrings(
        ".repair-shadow-77/indexes/dense_idx",
        reopened.entries.items[0].intent.previous_active_relative_path.?,
    );
}

test "index repair state rejects non-detected initial intents" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path = try checkpointPathAlloc(alloc, root);
    defer alloc.free(path);

    var state = try loadOrCreate(alloc, path, 2);
    const identity = state.identity;
    state.deinit(alloc);
    var building = try testEntry(alloc, identity, .building);
    defer building.deinit(alloc);
    try std.testing.expectError(error.InvalidIndexRepairTransition, putEntry(alloc, path, identity, null, building));
}

test "index repair state transitions are fenced and remove intent with pin" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path = try checkpointPathAlloc(alloc, root);
    defer alloc.free(path);

    var state = try loadOrCreate(alloc, path, 4);
    const identity = state.identity;
    state.deinit(alloc);
    var detected = try testEntry(alloc, identity, .detected);
    defer detected.deinit(alloc);
    try putEntry(alloc, path, identity, null, detected);

    var preflight = try testEntry(alloc, identity, .preflight);
    defer preflight.deinit(alloc);
    const expected: ExpectedTransition = .{
        .repair_id = 91,
        .revision = 1,
        .phase = .detected,
        .config_hash = 44,
        .root_generation = 4,
        .owner_epoch = 3,
    };
    try putEntry(alloc, path, identity, expected, preflight);
    try std.testing.expectError(error.RepairTransitionConflict, putEntry(alloc, path, identity, expected, preflight));

    var building = try testEntry(alloc, identity, .building);
    defer building.deinit(alloc);
    try putEntry(alloc, path, identity, .{
        .repair_id = 91,
        .revision = 2,
        .phase = .preflight,
        .config_hash = 44,
        .root_generation = 4,
        .owner_epoch = 3,
    }, building);

    var invalid = try testEntry(alloc, identity, .detected);
    defer invalid.deinit(alloc);
    try std.testing.expectError(error.InvalidIndexRepairTransition, putEntry(alloc, path, identity, .{
        .repair_id = 91,
        .revision = 3,
        .phase = .building,
        .config_hash = 44,
        .root_generation = 4,
        .owner_epoch = 3,
    }, invalid));

    try removeEntryAndPin(alloc, path, identity, .{
        .repair_id = 91,
        .revision = 3,
        .phase = .building,
        .config_hash = 44,
        .root_generation = 4,
        .owner_epoch = 3,
    });
    var empty = try load(alloc, path);
    defer empty.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty.entries.items.len);
}

test "index repair state revision fences same-phase mutations" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path = try checkpointPathAlloc(alloc, root);
    defer alloc.free(path);

    var state = try loadOrCreate(alloc, path, 4);
    const identity = state.identity;
    state.deinit(alloc);
    var initial = try testEntry(alloc, identity, .detected);
    defer initial.deinit(alloc);
    try putEntry(alloc, path, identity, null, initial);

    var first_snapshot = try load(alloc, path);
    defer first_snapshot.deinit(alloc);
    var paused = try first_snapshot.entries.items[0].clone(alloc);
    defer paused.deinit(alloc);
    paused.intent.automation = .paused;
    const expected: ExpectedTransition = .{
        .repair_id = paused.intent.repair_id,
        .revision = paused.intent.revision,
        .phase = paused.intent.phase,
        .config_hash = paused.intent.config_hash,
        .root_generation = paused.intent.root_generation,
        .owner_epoch = paused.intent.owner_epoch,
    };
    try putEntry(alloc, path, identity, expected, paused);

    var stale = try first_snapshot.entries.items[0].clone(alloc);
    defer stale.deinit(alloc);
    stale.intent.target_sequence = 99;
    try std.testing.expectError(error.RepairTransitionConflict, putEntry(alloc, path, identity, expected, stale));

    var current = try load(alloc, path);
    defer current.deinit(alloc);
    try std.testing.expectEqual(Automation.paused, current.entries.items[0].intent.automation);
    try std.testing.expectEqual(@as(u64, 2), current.entries.items[0].intent.revision);
}

test "index repair state root-generation reset atomically rebinds replacement debt" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path = try checkpointPathAlloc(alloc, root);
    defer alloc.free(path);

    var initial = try loadOrCreate(alloc, path, 3);
    const old_identity = initial.identity;
    initial.deinit(alloc);
    var entry = try testEntry(alloc, old_identity, .detected);
    defer entry.deinit(alloc);
    try putEntry(alloc, path, old_identity, null, entry);

    var replacement = try resetForRootGenerationWithIntents(alloc, path, old_identity, 4, &.{entry.intent});
    defer replacement.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4), replacement.identity.root_generation);
    try std.testing.expect(!replacement.identity.eql(old_identity));
    try std.testing.expectEqual(@as(usize, 1), replacement.entries.items.len);
    try std.testing.expect(replacement.entries.items[0].intent.identity().eql(replacement.identity));
    try std.testing.expectEqualStrings("dense_idx", replacement.entries.items[0].intent.index_name);
    try std.testing.expectError(error.ReplicaIdentityMismatch, resetForRootGeneration(alloc, path, old_identity, 5));
}

test "index repair state rejects escaped candidates and corrupt checkpoints" {
    try std.testing.expectError(error.InvalidRepairCandidatePath, validateCandidateRelativePath("dense_idx", ".repair-shadow-1/../dense_idx"));
    try std.testing.expectError(error.InvalidRepairCandidatePath, validateCandidateRelativePath("dense_idx", "/tmp/.repair-shadow-1/indexes/dense_idx"));
    try validateCandidateRelativePath("dense_idx", ".repair-shadow-1/indexes/dense_idx");

    const alloc = std.testing.allocator;
    const identity = try newReplicaIdentity(alloc, 1);
    var state = State{ .identity = identity };
    defer state.deinit(alloc);
    const raw = try encode(alloc, &state);
    defer alloc.free(raw);
    raw[magic.len] ^= 0xff;
    try std.testing.expectError(error.InvalidIndexRepairState, decode(alloc, raw));
}
