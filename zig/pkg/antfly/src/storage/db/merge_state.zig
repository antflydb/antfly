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

pub const key = "raftmerge:state";

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

    pub fn deinit(self: *State, alloc: std.mem.Allocator) void {
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
    };
}

pub const ApplyPlan = struct {
    state: State,
    range: db_types.ByteRange,
};

/// Validate and monotonically fold one receiver-side data-Raft checkpoint.
/// Replayed or delayed commands may be idempotent, but can never move the
/// durable phase, range, or donor watermark backwards.
pub fn planCheckpointApply(
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
    if ((checkpoint.kind == .accept or checkpoint.kind == .rollback) and
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

    const expected_current = switch (prior.phase) {
        .accepting => merged,
        .finalized => merged,
        .rolling_back, .rolled_back => base,
        .none => return error.InvalidMergeState,
    };
    if (!rangesEqual(current_range, expected_current)) return error.MergeRangeStateMismatch;

    switch (checkpoint.kind) {
        .accept => switch (prior.phase) {
            .accepting, .finalized => return preserveAdvanced(prior, checkpoint, expected_current),
            .rolling_back, .rolled_back => return error.ConflictingMergeTransition,
            .none => unreachable,
        },
        .bootstrap_complete => switch (prior.phase) {
            .accepting => {
                if (prior.bootstrap_complete and
                    checkpoint.bootstrap_applied_index <= prior.bootstrap_applied_index)
                    return preserveAdvanced(prior, checkpoint, merged);
                return .{
                    .state = stateFromCheckpoint(
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
                    .state = stateFromCheckpoint(
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
                .state = stateFromCheckpoint(checkpoint, .rolled_back, false, 0),
                .range = base,
            },
            .rolled_back => return preserveAdvanced(prior, checkpoint, base),
            .finalized => return error.ConflictingMergeTransition,
            .none => unreachable,
        },
    }
}

fn stateFromCheckpoint(
    checkpoint: db_types.MergeReplicationCheckpoint,
    phase: Phase,
    bootstrap_complete: bool,
    bootstrap_applied_index: u64,
) State {
    return .{
        .transition_id = checkpoint.transition_id,
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
