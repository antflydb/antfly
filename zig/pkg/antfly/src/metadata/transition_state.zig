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
const data = @import("../data/mod.zig");
const raft_state_machine = @import("../raft/state_machine/mod.zig");

pub const TransitionKind = enum {
    split,
    merge,
};

pub const TransitionPhase = enum {
    prepare,
    bootstrap_peer,
    replay_deltas,
    cutover_pending,
    finalizing,
    finalized,
    rolling_back,
    rolled_back,
};

pub const SplitTransitionRecord = struct {
    transition_id: u64,
    source_group_id: u64,
    destination_group_id: u64,
    phase: TransitionPhase = .prepare,
    split_key: ?[]const u8 = null,
    source_range_end: ?[]const u8 = null,
    rollback_reason: ?[]const u8 = null,
};

pub const MergeTransitionRecord = struct {
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    phase: TransitionPhase = .prepare,
    rollback_reason: ?[]const u8 = null,
    allow_doc_identity_reassignment: bool = false,
};

pub const SplitObservation = struct {
    status: data.SplitTransitionStatus,
    source_local_leader: bool = false,
    destination_local_leader: bool = false,
};

pub const MergeObservation = struct {
    donor: data.MergeTransitionStatus,
    receiver: data.MergeTransitionStatus,
    donor_local_leader: bool = false,
    receiver_local_leader: bool = false,
};

pub const SplitObservationRecord = struct {
    transition_id: u64,
    observation: SplitObservation,
};

pub const MergeObservationRecord = struct {
    transition_id: u64,
    observation: MergeObservation,
};

pub const GroupTransitionReadiness = struct {
    transition_pending: bool = false,
    replay_required: bool = false,
    replay_caught_up: bool = false,
    cutover_ready: bool = false,
    reads_ready_after_cutover: bool = false,
};

pub const GroupTransitionReadinessSource = enum {
    phase,
    metadata_observation,
    local_observation,
};

pub const GroupTransitionReadinessResult = struct {
    readiness: GroupTransitionReadiness,
    source: GroupTransitionReadinessSource,
};

pub const TransitionRecord = union(TransitionKind) {
    split: SplitTransitionRecord,
    merge: MergeTransitionRecord,
};

pub const TransitionObservation = union(TransitionKind) {
    split: SplitObservation,
    merge: MergeObservation,
};

pub fn readinessForGroup(
    group_id: u64,
    split_transitions: []const SplitTransitionRecord,
    merge_transitions: []const MergeTransitionRecord,
) GroupTransitionReadiness {
    var readiness = GroupTransitionReadiness{};
    for (split_transitions) |record| {
        if (record.source_group_id != group_id and record.destination_group_id != group_id) continue;
        readiness = combineReadiness(readiness, readinessFromPhase(record.phase));
    }
    for (merge_transitions) |record| {
        if (record.donor_group_id != group_id and record.receiver_group_id != group_id) continue;
        readiness = combineReadiness(readiness, readinessFromPhase(record.phase));
    }
    return readiness;
}

pub fn readinessForLocalGroup(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    split_transitions: []const SplitTransitionRecord,
    merge_transitions: []const MergeTransitionRecord,
    split_observations: []const SplitObservationRecord,
    merge_observations: []const MergeObservationRecord,
) !GroupTransitionReadiness {
    var readiness = GroupTransitionReadiness{};
    for (split_transitions) |record| {
        if (record.source_group_id != group_id and record.destination_group_id != group_id) continue;
        readiness = combineReadiness(readiness, (try readinessResultForLocalSplitTransition(
            alloc,
            replica_root_dir,
            record,
            split_observations,
        )).readiness);
    }
    for (merge_transitions) |record| {
        if (record.donor_group_id != group_id and record.receiver_group_id != group_id) continue;
        readiness = combineReadiness(readiness, (try readinessResultForLocalMergeTransition(
            alloc,
            replica_root_dir,
            record,
            merge_observations,
        )).readiness);
    }
    return readiness;
}

fn combineReadiness(
    current: GroupTransitionReadiness,
    next: GroupTransitionReadiness,
) GroupTransitionReadiness {
    return .{
        .transition_pending = current.transition_pending or next.transition_pending,
        .replay_required = current.replay_required or next.replay_required,
        .replay_caught_up = current.replay_caught_up or next.replay_caught_up,
        .cutover_ready = current.cutover_ready or next.cutover_ready,
        .reads_ready_after_cutover = current.reads_ready_after_cutover or next.reads_ready_after_cutover,
    };
}

pub fn readinessResultForLocalSplitTransition(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    record: SplitTransitionRecord,
    split_observations: []const SplitObservationRecord,
) !GroupTransitionReadinessResult {
    if (try observeLocalSplitTransition(alloc, replica_root_dir, record)) |status| {
        return .{
            .readiness = readinessFromObservedLocalSplit(status),
            .source = .local_observation,
        };
    }
    if (findSplitObservation(split_observations, record.transition_id)) |observation| {
        return .{
            .readiness = readinessFromObservedSplit(observation.status),
            .source = .metadata_observation,
        };
    }
    return .{
        .readiness = readinessFromPhase(record.phase),
        .source = .phase,
    };
}

pub fn readinessResultForLocalMergeTransition(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    record: MergeTransitionRecord,
    merge_observations: []const MergeObservationRecord,
) !GroupTransitionReadinessResult {
    if (try observeLocalMergeTransition(alloc, replica_root_dir, record)) |status| {
        return .{
            .readiness = readinessFromObservedMerge(status),
            .source = .local_observation,
        };
    }
    if (findMergeObservation(merge_observations, record.transition_id)) |observation| {
        return .{
            .readiness = readinessFromObservedMerge(observation.receiver),
            .source = .metadata_observation,
        };
    }
    return .{
        .readiness = readinessFromPhase(record.phase),
        .source = .phase,
    };
}

fn readinessFromPhase(phase: TransitionPhase) GroupTransitionReadiness {
    return .{
        .transition_pending = true,
        .replay_required = switch (phase) {
            .bootstrap_peer, .replay_deltas, .cutover_pending, .finalizing => true,
            else => false,
        },
        .replay_caught_up = switch (phase) {
            .cutover_pending, .finalizing, .finalized => true,
            else => false,
        },
        .cutover_ready = switch (phase) {
            .cutover_pending, .finalizing, .finalized => true,
            else => false,
        },
        .reads_ready_after_cutover = switch (phase) {
            .cutover_pending, .finalizing, .finalized => true,
            else => false,
        },
    };
}

fn readinessFromObservedSplit(status: data.SplitTransitionStatus) GroupTransitionReadiness {
    return .{
        .transition_pending = status.phase != .finalized and status.phase != .rolled_back,
        .replay_required = status.replay_required,
        .replay_caught_up = status.replay_caught_up,
        .cutover_ready = status.cutover_ready,
        .reads_ready_after_cutover = status.destination_ready_for_reads,
    };
}

fn readinessFromObservedLocalSplit(status: data.SplitSyncStatus) GroupTransitionReadiness {
    return .{
        .transition_pending = status.phase != .finalized and status.phase != .rolled_back,
        .replay_required = status.replay_required,
        .replay_caught_up = status.replay_caught_up,
        .cutover_ready = status.cutover_ready,
        .reads_ready_after_cutover = status.destination_ready_for_reads,
    };
}

fn readinessFromObservedMerge(status: data.MergeTransitionStatus) GroupTransitionReadiness {
    return .{
        .transition_pending = status.phase != .finalized and status.phase != .rolled_back,
        .replay_required = status.replay_required,
        .replay_caught_up = status.replay_caught_up,
        .cutover_ready = status.cutover_ready,
        .reads_ready_after_cutover = status.receiver_ready_for_reads,
    };
}

fn observeLocalSplitTransition(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    record: SplitTransitionRecord,
) !?data.SplitSyncStatus {
    const source_root_dir = try groupDbPathAlloc(alloc, replica_root_dir, record.source_group_id);
    defer alloc.free(source_root_dir);
    const dest_root_dir = try groupDbPathAlloc(alloc, replica_root_dir, record.destination_group_id);
    defer alloc.free(dest_root_dir);

    if (!try pathExists(alloc, source_root_dir) or !try pathExists(alloc, dest_root_dir)) return null;

    var coord = try data.SplitSyncCoordinator.init(alloc, .{
        .source_root_dir = source_root_dir,
        .dest_root_dir = dest_root_dir,
        .source_group_id = record.source_group_id,
        .dest_group_id = record.destination_group_id,
    });
    defer coord.deinit();
    return try coord.status();
}

fn observeLocalMergeTransition(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    record: MergeTransitionRecord,
) !?data.MergeTransitionStatus {
    const donor_root_dir = try groupDbPathAlloc(alloc, replica_root_dir, record.donor_group_id);
    defer alloc.free(donor_root_dir);
    const receiver_root_dir = try groupDbPathAlloc(alloc, replica_root_dir, record.receiver_group_id);
    defer alloc.free(receiver_root_dir);

    if (!try pathExists(alloc, donor_root_dir) or !try pathExists(alloc, receiver_root_dir)) return null;

    var coord = try data.MergeCoordinator.init(alloc, .{
        .donor_root_dir = donor_root_dir,
        .receiver_root_dir = receiver_root_dir,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
    });
    defer coord.deinit();
    return try coord.status();
}

fn groupDbPathAlloc(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ replica_root_dir, group_id });
}

fn pathExists(alloc: std.mem.Allocator, path: []const u8) !bool {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn findSplitObservation(
    records: []const SplitObservationRecord,
    transition_id: u64,
) ?SplitObservation {
    for (records) |record| {
        if (record.transition_id == transition_id) return record.observation;
    }
    return null;
}

fn findMergeObservation(
    records: []const MergeObservationRecord,
    transition_id: u64,
) ?MergeObservation {
    for (records) |record| {
        if (record.transition_id == transition_id) return record.observation;
    }
    return null;
}

test "transition state module compiles" {
    _ = TransitionKind;
    _ = TransitionPhase;
    _ = SplitTransitionRecord;
    _ = MergeTransitionRecord;
    _ = SplitObservation;
    _ = MergeObservation;
    _ = SplitObservationRecord;
    _ = MergeObservationRecord;
    _ = TransitionRecord;
    _ = TransitionObservation;
    _ = GroupTransitionReadiness;
    _ = GroupTransitionReadinessSource;
    _ = GroupTransitionReadinessResult;
    _ = readinessForGroup;
    _ = readinessForLocalGroup;
    _ = readinessResultForLocalSplitTransition;
    _ = readinessResultForLocalMergeTransition;
}

test "transition state derives heartbeat readiness for active phases" {
    const readiness = readinessForGroup(12, &[_]SplitTransitionRecord{.{
        .transition_id = 9001,
        .source_group_id = 10,
        .destination_group_id = 12,
        .phase = .cutover_pending,
    }}, &.{});
    try std.testing.expect(readiness.transition_pending);
    try std.testing.expect(readiness.replay_required);
    try std.testing.expect(readiness.replay_caught_up);
    try std.testing.expect(readiness.cutover_ready);
    try std.testing.expect(readiness.reads_ready_after_cutover);
}

test "transition state leaves readiness empty for unrelated groups" {
    const readiness = readinessForGroup(99, &[_]SplitTransitionRecord{.{
        .transition_id = 9001,
        .source_group_id = 10,
        .destination_group_id = 12,
        .phase = .prepare,
    }}, &[_]MergeTransitionRecord{.{
        .transition_id = 9002,
        .donor_group_id = 20,
        .receiver_group_id = 21,
        .phase = .bootstrap_peer,
    }});
    try std.testing.expect(!readiness.transition_pending);
    try std.testing.expect(!readiness.replay_required);
    try std.testing.expect(!readiness.replay_caught_up);
    try std.testing.expect(!readiness.cutover_ready);
    try std.testing.expect(!readiness.reads_ready_after_cutover);
}

test "transition state prefers observed local split readiness over metadata phase" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/transition-state-local", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root_dir);
    const source_root_dir = try groupDbPathAlloc(std.testing.allocator, replica_root_dir, 131);
    defer std.testing.allocator.free(source_root_dir);
    const dest_root_dir = try groupDbPathAlloc(std.testing.allocator, replica_root_dir, 132);
    defer std.testing.allocator.free(dest_root_dir);

    var source = try data.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root_dir });
    defer source.deinit();

    const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
    });
    defer std.testing.allocator.free(prepare);
    try source.snapshotBuilder().applyBatch(.{
        .group_id = 131,
        .commit_index = 4,
        .entries_bytes = prepare,
    });

    var coord = try data.SplitSyncCoordinator.init(std.testing.allocator, .{
        .source_root_dir = source_root_dir,
        .dest_root_dir = dest_root_dir,
        .source_group_id = 131,
        .dest_group_id = 132,
    });
    defer coord.deinit();

    const start = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("split_start:132:doc:m") },
    });
    defer std.testing.allocator.free(start);
    try coord.source.snapshotBuilder().applyBatch(.{
        .group_id = 131,
        .commit_index = 5,
        .entries_bytes = start,
    });
    _ = try coord.syncOnce();

    const readiness = try readinessForLocalGroup(
        std.testing.allocator,
        replica_root_dir,
        132,
        &[_]SplitTransitionRecord{.{
            .transition_id = 1001,
            .source_group_id = 131,
            .destination_group_id = 132,
            .phase = .bootstrap_peer,
            .split_key = "doc:m",
        }},
        &.{},
        &.{},
        &.{},
    );
    try std.testing.expect(readiness.transition_pending);
    try std.testing.expect(readiness.replay_required);
    try std.testing.expect(readiness.replay_caught_up);
    try std.testing.expect(readiness.cutover_ready);
    try std.testing.expect(readiness.reads_ready_after_cutover);
}

test "transition state falls back to metadata observation when local pair is absent" {
    const readiness = try readinessForLocalGroup(
        std.testing.allocator,
        ".zig-cache/tmp/nonexistent-transition-state-root",
        212,
        &[_]SplitTransitionRecord{.{
            .transition_id = 2001,
            .source_group_id = 211,
            .destination_group_id = 212,
            .phase = .bootstrap_peer,
            .split_key = "doc:m",
        }},
        &.{},
        &[_]SplitObservationRecord{.{
            .transition_id = 2001,
            .observation = .{
                .status = .{
                    .phase = .cutover_ready,
                    .source_split_phase = .splitting,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 5,
                    .dest_delta_sequence = 5,
                },
            },
        }},
        &.{},
    );
    try std.testing.expect(readiness.transition_pending);
    try std.testing.expect(readiness.replay_required);
    try std.testing.expect(readiness.replay_caught_up);
    try std.testing.expect(readiness.cutover_ready);
    try std.testing.expect(readiness.reads_ready_after_cutover);
}
