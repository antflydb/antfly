// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the ELv2 at https://www.antfly.io/licensing/ELv2-license

//! Durable receiver-side range-merge state shared by the direct coordinator
//! and data-Raft apply. Keeping one codec is required for leader failover: a
//! follower that applies a replicated checkpoint must be observable by the
//! ordinary MergeCoordinator after promotion.

const std = @import("std");
const db_types = @import("types.zig");
const doc_identity = @import("doc_identity.zig");
const docstore = @import("../docstore.zig");

// Group-owned metadata must sort before document keys so a physical LSM
// split retains it on the parent. Split destinations clear this prefix.
pub const key = "\x00\x00__metadata__:raftmerge";
pub const legacy_key = "raftmerge:state";

pub fn loadRawAlloc(alloc: std.mem.Allocator, store: *docstore.DocStore) !?[]u8 {
    return store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => store.get(alloc, legacy_key) catch |legacy_err| switch (legacy_err) {
            error.NotFound => null,
            else => return legacy_err,
        },
        else => return err,
    };
}

/// Upgrade existing production receipts before a physical split can move or
/// discard the old key. The protected copy and old-key deletion commit in one
/// batch and are synced before the destructive rewrite, not restored after it.
pub fn protectForSplit(alloc: std.mem.Allocator, store: *docstore.DocStore) !void {
    const old = store.get(alloc, legacy_key) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    defer alloc.free(old);
    const current = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (current) |value| alloc.free(value);
    const raw = current orelse old;
    var state = try decodeAlloc(alloc, raw);
    defer state.deinit(alloc);
    try store.putBatch(&.{.{ .key = key, .value = raw }}, &.{legacy_key});
    try store.sync(true);
}

pub const Phase = enum(u8) {
    none = 0,
    accepting = 1,
    finalized = 2,
    rolling_back = 3,
    rolled_back = 4,
};

pub const State = struct {
    transition_id: u64 = 0,
    donor_group_id: u64,
    receiver_group_id: u64,
    phase: Phase,
    receiver_base_range: db_types.ByteRange,
    /// The exact range accepted by this transition. Older records did not
    /// carry it; the next checkpoint binds those records before advancing.
    merged_range: ?db_types.ByteRange = null,
    allow_doc_identity_reassignment: bool = false,
    receiver_identity_reassignment_namespace: ?doc_identity.Namespace = null,
    bootstrap_complete: bool = false,
    bootstrap_applied_index: u64 = 0,
    copy_attempt: db_types.MergeCopyAttempt = .{},
    /// Terminal identities remain fenced even after another merge takes over.
    retired_transition_ids: []const u64 = &.{},

    pub fn deinit(self: *State, alloc: std.mem.Allocator) void {
        alloc.free(self.retired_transition_ids);
        alloc.free(@constCast(self.receiver_base_range.start));
        alloc.free(@constCast(self.receiver_base_range.end));
        if (self.merged_range) |merged| {
            alloc.free(@constCast(merged.start));
            alloc.free(@constCast(merged.end));
        }
        self.* = undefined;
    }
};

pub fn encode(
    list: *std.ArrayListUnmanaged(u8),
    alloc: std.mem.Allocator,
    state: State,
) !void {
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
    try list.append(alloc, if (state.bootstrap_complete) 1 else 0);
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, state.bootstrap_applied_index)));
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, state.transition_id)));
    if (state.merged_range) |merged| {
        try list.append(alloc, 1);
        const merged_start_len: u32 = @intCast(merged.start.len);
        try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u32, merged_start_len)));
        try list.appendSlice(alloc, merged.start);
        const merged_end_len: u32 = @intCast(merged.end.len);
        try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u32, merged_end_len)));
        try list.appendSlice(alloc, merged.end);
    } else {
        try list.append(alloc, 0);
    }
    const retired_len: u32 = @intCast(state.retired_transition_ids.len);
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u32, retired_len)));
    for (state.retired_transition_ids) |id|
        try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, id)));
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, state.copy_attempt.donor_term)));
    try list.appendSlice(alloc, std.mem.asBytes(&std.mem.nativeToLittle(u64, state.copy_attempt.sequence)));
}

pub fn decodeAlloc(alloc: std.mem.Allocator, data: []const u8) !State {
    if (data.len < 1 + 8 + 8 + 4 + 4) return error.InvalidMergeState;
    var pos: usize = 0;
    if (data[pos] > @intFromEnum(Phase.rolled_back)) return error.InvalidMergeState;
    const phase: Phase = @enumFromInt(data[pos]);
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
    if (data.len - pos < 4) return error.InvalidMergeState;
    const end_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (pos + end_len > data.len) return error.InvalidMergeState;
    const end = try alloc.dupe(u8, data[pos .. pos + end_len]);
    errdefer alloc.free(end);
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
        if (table_id == 0 or shard_id == 0 or range_id == 0) return error.InvalidMergeState;
        break :blk .{ .table_id = table_id, .shard_id = shard_id, .range_id = range_id };
    } else null;
    const bootstrap_complete = if (pos < data.len) blk: {
        const complete = data[pos] != 0;
        pos += 1;
        break :blk complete;
    } else false;
    const bootstrap_applied_index = if (pos < data.len) blk: {
        if (pos + 8 > data.len) return error.InvalidMergeState;
        const index = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        break :blk index;
    } else 0;
    const transition_id = if (pos < data.len) blk: {
        if (pos + 8 > data.len) return error.InvalidMergeState;
        const value = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        break :blk value;
    } else 0;
    var merged_start: ?[]u8 = null;
    errdefer if (merged_start) |value| alloc.free(value);
    var merged_end: ?[]u8 = null;
    errdefer if (merged_end) |value| alloc.free(value);
    const merged_range: ?db_types.ByteRange = if (pos < data.len) blk: {
        const has_merged_range = data[pos] != 0;
        pos += 1;
        if (!has_merged_range) break :blk null;
        if (pos + 4 > data.len) return error.InvalidMergeState;
        const merged_start_len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        if (pos + merged_start_len > data.len) return error.InvalidMergeState;
        merged_start = try alloc.dupe(u8, data[pos .. pos + merged_start_len]);
        pos += merged_start_len;
        if (pos + 4 > data.len) return error.InvalidMergeState;
        const merged_end_len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        if (pos + merged_end_len > data.len) return error.InvalidMergeState;
        merged_end = try alloc.dupe(u8, data[pos .. pos + merged_end_len]);
        pos += merged_end_len;
        break :blk .{ .start = merged_start.?, .end = merged_end.? };
    } else null;
    var retired: []u64 = &.{};
    errdefer alloc.free(retired);
    if (pos < data.len) {
        if (data.len - pos < 4) return error.InvalidMergeState;
        const count = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        if (count > (data.len - pos) / 8) return error.InvalidMergeState;
        retired = try alloc.alloc(u64, count);
        for (retired) |*id| {
            id.* = std.mem.readInt(u64, data[pos..][0..8], .little);
            pos += 8;
        }
    }
    var copy_attempt: db_types.MergeCopyAttempt = .{};
    if (pos < data.len) {
        if (data.len - pos != 16) return error.InvalidMergeState;
        copy_attempt.donor_term = std.mem.readInt(u64, data[pos..][0..8], .little);
        copy_attempt.sequence = std.mem.readInt(u64, data[pos + 8 ..][0..8], .little);
        pos += 16;
    }
    if (pos != data.len or donor_group_id == 0 or receiver_group_id == 0 or
        donor_group_id == receiver_group_id)
        return error.InvalidMergeState;
    return .{
        .transition_id = transition_id,
        .donor_group_id = donor_group_id,
        .receiver_group_id = receiver_group_id,
        .phase = phase,
        .receiver_base_range = .{ .start = start, .end = end },
        .merged_range = merged_range,
        .allow_doc_identity_reassignment = allow_doc_identity_reassignment,
        .receiver_identity_reassignment_namespace = receiver_identity_reassignment_namespace,
        .bootstrap_complete = bootstrap_complete,
        .bootstrap_applied_index = bootstrap_applied_index,
        .retired_transition_ids = retired,
        .copy_attempt = copy_attempt,
    };
}

pub const ApplyPlan = struct {
    state: State,
    range: db_types.ByteRange,
    owned_retired_ids: ?[]u64 = null,

    pub fn deinit(self: ApplyPlan, alloc: std.mem.Allocator) void {
        if (self.owned_retired_ids) |ids| alloc.free(ids);
    }
};

pub fn isRetired(state: State, transition_id: u64) bool {
    return std.mem.indexOfScalar(u64, state.retired_transition_ids, transition_id) != null;
}

pub fn retireCurrentAlloc(alloc: std.mem.Allocator, state: State) ![]u64 {
    const ids = try alloc.alloc(u64, state.retired_transition_ids.len + 1);
    @memcpy(ids[0..state.retired_transition_ids.len], state.retired_transition_ids);
    ids[ids.len - 1] = state.transition_id;
    return ids;
}

/// Copy payloads have authority only during their exact receiver transition.
/// A stale committed payload is a no-op, not a fatal Raft apply error.
pub fn copyAllowed(state: ?State, replication: db_types.MergeReplicationContext) bool {
    const current = state orelse return false;
    return current.phase == .accepting and !current.bootstrap_complete and
        current.copy_attempt.order(replication.copy_attempt) == .eq and
        current.transition_id == replication.transition_id and
        current.donor_group_id == replication.donor_group_id and
        current.receiver_group_id == replication.receiver_group_id;
}

/// Validate and monotonically fold one receiver-side data-Raft checkpoint.
/// Replayed or delayed commands may be idempotent, but can never move the
/// durable phase, range, or donor watermark backwards.
pub fn planCheckpointApply(
    alloc: std.mem.Allocator,
    existing: ?*const State,
    current_range: db_types.ByteRange,
    checkpoint: db_types.MergeReplicationCheckpoint,
) !ApplyPlan {
    const base: db_types.ByteRange = .{
        .start = checkpoint.receiver_base_start,
        .end = checkpoint.receiver_base_end,
    };
    const merged: db_types.ByteRange = .{
        .start = checkpoint.merged_start,
        .end = checkpoint.merged_end,
    };
    if (checkpoint.transition_id == 0 or checkpoint.donor_group_id == 0 or
        checkpoint.receiver_group_id == 0 or
        checkpoint.donor_group_id == checkpoint.receiver_group_id or
        !validRange(base) or !validRange(merged) or !rangeContains(merged, base) or
        rangesEqual(base, merged))
        return error.InvalidMergeCheckpoint;
    if ((checkpoint.kind == .accept or checkpoint.kind == .begin_copy or checkpoint.kind == .rollback) and
        checkpoint.bootstrap_applied_index != 0)
        return error.InvalidMergeCheckpoint;
    if ((checkpoint.kind == .bootstrap_complete or checkpoint.kind == .finalize) and
        checkpoint.bootstrap_applied_index == 0)
        return error.InvalidMergeCheckpoint;
    if (checkpoint.allow_doc_identity_reassignment !=
        (checkpoint.receiver_identity_reassignment_namespace != null))
        return error.InvalidMergeCheckpoint;

    if (existing == null) {
        if (checkpoint.kind != .accept or !rangesEqual(current_range, base))
            return error.MergeTransitionNotReady;
        return .{
            .state = stateFromCheckpoint(checkpoint, .accepting, false, 0),
            // Public routing remains on the metadata-owned base range, while
            // the private receiver generation must accept donor writes as soon
            // as bootstrap begins.
            .range = merged,
        };
    }

    const prior = existing.?;
    if (isRetired(prior.*, checkpoint.transition_id))
        return .{ .state = prior.*, .range = current_range };
    // Both terminal outcomes release the receiver for a fresh transition.
    // Retain retired identities so delayed accepts cannot resurrect them.
    if ((prior.phase == .rolled_back or prior.phase == .finalized) and
        prior.transition_id != checkpoint.transition_id)
    {
        if (checkpoint.kind != .accept or !rangesEqual(current_range, base))
            return error.ConflictingMergeTransition;
        const retired = try retireCurrentAlloc(alloc, prior.*);
        var next = stateFromCheckpoint(checkpoint, .accepting, false, 0);
        next.retired_transition_ids = retired;
        return .{
            .state = next,
            .range = merged,
            .owned_retired_ids = retired,
        };
    }
    if ((prior.transition_id != 0 and prior.transition_id != checkpoint.transition_id) or
        prior.donor_group_id != checkpoint.donor_group_id or
        prior.receiver_group_id != checkpoint.receiver_group_id or
        !rangesEqual(prior.receiver_base_range, base) or
        (prior.merged_range != null and !rangesEqual(prior.merged_range.?, merged)) or
        prior.allow_doc_identity_reassignment != checkpoint.allow_doc_identity_reassignment or
        !optionalNamespaceEqual(
            prior.receiver_identity_reassignment_namespace,
            checkpoint.receiver_identity_reassignment_namespace,
        ))
        return error.ConflictingMergeTransition;

    // An exact terminal identity is a durable no-op for delayed controls, not
    // a command error that can wedge replay at an already committed index.
    // A subsequent split may have changed the live range; only the historical
    // identity/range/namespace contract above must still match the receipt.
    if (prior.phase == .finalized or prior.phase == .rolled_back)
        return preserveAdvanced(prior, checkpoint, current_range);

    const expected_current = switch (prior.phase) {
        .accepting => merged,
        .rolling_back => base,
        .finalized, .rolled_back => unreachable,
        .none => return error.InvalidMergeState,
    };
    if (!rangesEqual(current_range, expected_current)) return error.MergeRangeStateMismatch;

    // Attempts are ordered first by the donor's elected term, then by its
    // node-local sequence. Delayed begins cannot reclaim a newer attempt;
    // delayed completion/finalization cannot certify a different copy.
    if (checkpoint.kind == .begin_copy) {
        if (checkpoint.copy_attempt.donor_term == 0 or checkpoint.copy_attempt.sequence == 0)
            return error.InvalidMergeCheckpoint;
        if (prior.phase != .accepting or checkpoint.copy_attempt.order(prior.copy_attempt) != .gt)
            return .{ .state = prior.*, .range = current_range };
        return .{
            .state = advanceState(prior, checkpoint, .accepting, false, 0),
            .range = merged,
        };
    }
    if (checkpoint.kind != .accept and checkpoint.copy_attempt.order(prior.copy_attempt) != .eq)
        return .{ .state = prior.*, .range = current_range };

    switch (checkpoint.kind) {
        .begin_copy => unreachable,
        .accept => switch (prior.phase) {
            .accepting, .finalized => return preserveAdvanced(prior, checkpoint, expected_current),
            .rolling_back, .rolled_back => return preserveAdvanced(prior, checkpoint, expected_current),
            .none => unreachable,
        },
        .bootstrap_complete => switch (prior.phase) {
            .accepting => {
                if (prior.bootstrap_complete and
                    checkpoint.bootstrap_applied_index <= prior.bootstrap_applied_index)
                    return preserveAdvanced(prior, checkpoint, merged);
                return .{
                    .state = advanceState(
                        prior,
                        checkpoint,
                        .accepting,
                        true,
                        checkpoint.bootstrap_applied_index,
                    ),
                    .range = merged,
                };
            },
            .finalized => {
                if (checkpoint.bootstrap_applied_index > prior.bootstrap_applied_index)
                    return error.ConflictingMergeTransition;
                return preserveAdvanced(prior, checkpoint, merged);
            },
            .rolling_back, .rolled_back => return error.ConflictingMergeTransition,
            .none => unreachable,
        },
        .finalize => switch (prior.phase) {
            .accepting => {
                if (!prior.bootstrap_complete) return error.MergeTransitionNotReady;
                return .{
                    .state = advanceState(
                        prior,
                        checkpoint,
                        .finalized,
                        true,
                        @max(prior.bootstrap_applied_index, checkpoint.bootstrap_applied_index),
                    ),
                    .range = merged,
                };
            },
            .finalized => {
                if (checkpoint.bootstrap_applied_index > prior.bootstrap_applied_index)
                    return error.ConflictingMergeTransition;
                return preserveAdvanced(prior, checkpoint, merged);
            },
            .rolling_back, .rolled_back => return error.ConflictingMergeTransition,
            .none => unreachable,
        },
        .rollback => switch (prior.phase) {
            .accepting, .rolling_back => return .{
                .state = advanceState(prior, checkpoint, .rolled_back, false, 0),
                .range = base,
            },
            .rolled_back => return preserveAdvanced(prior, checkpoint, base),
            .finalized => return error.ConflictingMergeTransition,
            .none => unreachable,
        },
    }
}

test "rolled back merge receiver admits only a fresh accept transition" {
    const prior = State{
        .transition_id = 500,
        .donor_group_id = 501,
        .receiver_group_id = 502,
        .phase = .rolled_back,
        .receiver_base_range = .{ .start = "doc:m", .end = "" },
        .merged_range = .{ .start = "doc:a", .end = "" },
    };
    const fresh = db_types.MergeReplicationCheckpoint{
        .kind = .accept,
        .transition_id = 600,
        .donor_group_id = 601,
        .receiver_group_id = 502,
        .receiver_base_start = "doc:m",
        .receiver_base_end = "",
        .merged_start = "doc:a",
        .merged_end = "",
    };

    const accepted = try planCheckpointApply(
        std.testing.allocator,
        &prior,
        .{ .start = "doc:m", .end = "" },
        fresh,
    );
    defer accepted.deinit(std.testing.allocator);
    try std.testing.expectEqual(Phase.accepting, accepted.state.phase);
    try std.testing.expectEqual(@as(u64, 600), accepted.state.transition_id);
    try std.testing.expectEqual(@as(u64, 601), accepted.state.donor_group_id);
    try std.testing.expectEqualStrings("doc:a", accepted.range.start);

    var not_accept = fresh;
    not_accept.kind = .bootstrap_complete;
    not_accept.bootstrap_applied_index = 7;
    try std.testing.expectError(
        error.ConflictingMergeTransition,
        planCheckpointApply(std.testing.allocator, &prior, .{ .start = "doc:m", .end = "" }, not_accept),
    );
    try std.testing.expectError(
        error.ConflictingMergeTransition,
        planCheckpointApply(std.testing.allocator, &prior, .{ .start = "doc:n", .end = "" }, fresh),
    );
}

fn advanceState(prior: *const State, checkpoint: db_types.MergeReplicationCheckpoint, phase: Phase, complete: bool, index: u64) State {
    var state = stateFromCheckpoint(checkpoint, phase, complete, index);
    state.retired_transition_ids = prior.retired_transition_ids;
    return state;
}

fn stateFromCheckpoint(
    checkpoint: db_types.MergeReplicationCheckpoint,
    phase: Phase,
    bootstrap_complete: bool,
    bootstrap_applied_index: u64,
) State {
    return .{
        .transition_id = checkpoint.transition_id,
        .copy_attempt = checkpoint.copy_attempt,
        .donor_group_id = checkpoint.donor_group_id,
        .receiver_group_id = checkpoint.receiver_group_id,
        .phase = phase,
        .receiver_base_range = .{
            .start = checkpoint.receiver_base_start,
            .end = checkpoint.receiver_base_end,
        },
        .merged_range = .{
            .start = checkpoint.merged_start,
            .end = checkpoint.merged_end,
        },
        .allow_doc_identity_reassignment = checkpoint.allow_doc_identity_reassignment,
        .receiver_identity_reassignment_namespace = checkpoint.receiver_identity_reassignment_namespace,
        .bootstrap_complete = bootstrap_complete,
        .bootstrap_applied_index = bootstrap_applied_index,
    };
}

fn preserveAdvanced(
    prior: *const State,
    checkpoint: db_types.MergeReplicationCheckpoint,
    range: db_types.ByteRange,
) ApplyPlan {
    var state = prior.*;
    if (state.transition_id == 0) state.transition_id = checkpoint.transition_id;
    if (state.merged_range == null) state.merged_range = .{
        .start = checkpoint.merged_start,
        .end = checkpoint.merged_end,
    };
    return .{ .state = state, .range = range };
}

fn optionalNamespaceEqual(
    left: ?doc_identity.Namespace,
    right: ?doc_identity.Namespace,
) bool {
    if (left == null or right == null) return left == null and right == null;
    return left.?.eql(right.?);
}

fn rangesEqual(left: db_types.ByteRange, right: db_types.ByteRange) bool {
    return std.mem.eql(u8, left.start, right.start) and std.mem.eql(u8, left.end, right.end);
}

fn validRange(range: db_types.ByteRange) bool {
    return range.end.len == 0 or std.mem.order(u8, range.start, range.end) == .lt;
}

fn rangeContains(outer: db_types.ByteRange, inner: db_types.ByteRange) bool {
    const starts_before = std.mem.order(u8, outer.start, inner.start) != .gt;
    const ends_after = if (outer.end.len == 0)
        true
    else if (inner.end.len == 0)
        false
    else
        std.mem.order(u8, outer.end, inner.end) != .lt;
    return starts_before and ends_after;
}
