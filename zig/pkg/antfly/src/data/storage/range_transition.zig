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
const shard_mod = @import("../../storage/shard.zig");

pub const TransitionKind = enum {
    split,
    merge,
};

pub const TransitionRole = enum {
    source,
    destination,
    donor,
    receiver,
};

pub const TransitionPhase = enum {
    prepare,
    bootstrap_peer,
    replay_deltas,
    cutover_ready,
    finalized,
    rolling_back,
    rolled_back,
};

pub const SplitStatus = struct {
    phase: TransitionPhase,
    source_split_phase: ?shard_mod.SplitPhase,
    bootstrapped: bool,
    replay_required: bool,
    replay_caught_up: bool,
    cutover_ready: bool,
    destination_ready_for_reads: bool,
    source_delta_sequence: u64,
    dest_delta_sequence: u64,
};

pub const MergeStatus = struct {
    phase: TransitionPhase,
    donor_group_id: u64,
    receiver_group_id: u64,
    receiver_accepts_donor_range: bool,
    bootstrapped: bool,
    replay_required: bool,
    replay_caught_up: bool,
    cutover_ready: bool,
    receiver_ready_for_reads: bool,
    donor_delta_sequence: u64,
    receiver_delta_sequence: u64,
    allow_doc_identity_reassignment: bool = false,
    receiver_identity_reassignment_namespace_table_id: u64 = 0,
    receiver_identity_reassignment_namespace_shard_id: u64 = 0,
    receiver_identity_reassignment_namespace_range_id: u64 = 0,
};

pub const MergeParticipantStatus = struct {
    role: TransitionRole,
    phase: TransitionPhase,
    donor_group_id: u64,
    receiver_group_id: u64,
    accepts_donor_range: bool,
    replay_required: bool,
    replay_caught_up: bool,
    cutover_ready: bool,
};

pub const MergePairError = error{
    InvalidParticipantRole,
    MismatchedPairIds,
    ReceiverMustAcceptDonorRange,
    DonorMustNotAcceptDonorRange,
    MismatchedPhase,
    MismatchedReplayRequirement,
    MismatchedReplayCaughtUp,
    MismatchedCutoverReadiness,
};

pub fn deriveSplitStatus(
    source_phase: ?shard_mod.SplitPhase,
    bootstrapped: bool,
    source_delta_sequence: u64,
    dest_delta_sequence: u64,
) SplitStatus {
    const replay_required = bootstrapped and source_phase != null and
        (source_phase.? == .splitting or source_phase.? == .finalizing);
    const replay_caught_up = if (replay_required) dest_delta_sequence >= source_delta_sequence else false;
    const phase: TransitionPhase = if (source_phase) |phase_value| switch (phase_value) {
        .prepare => .prepare,
        .splitting, .finalizing => if (!bootstrapped)
            .bootstrap_peer
        else if (dest_delta_sequence < source_delta_sequence)
            .replay_deltas
        else
            .cutover_ready,
        .rolling_back => .rolling_back,
        .none => if (bootstrapped) .finalized else .rolled_back,
    } else if (bootstrapped)
        .finalized
    else
        .rolled_back;

    return .{
        .phase = phase,
        .source_split_phase = source_phase,
        .bootstrapped = bootstrapped,
        .replay_required = replay_required,
        .replay_caught_up = replay_caught_up,
        .cutover_ready = phase == .cutover_ready or phase == .finalized,
        .destination_ready_for_reads = phase == .cutover_ready or phase == .finalized,
        .source_delta_sequence = source_delta_sequence,
        .dest_delta_sequence = dest_delta_sequence,
    };
}

pub fn deriveMergeStatus(
    donor_group_id: u64,
    receiver_group_id: u64,
    receiver_accepts_donor_range: bool,
    bootstrapped: bool,
    donor_delta_sequence: u64,
    receiver_delta_sequence: u64,
    rolling_back: bool,
    finalized: bool,
    rolled_back: bool,
) MergeStatus {
    const replay_required = bootstrapped and receiver_accepts_donor_range and !finalized and !rolling_back and !rolled_back;
    const replay_caught_up = if (replay_required) receiver_delta_sequence >= donor_delta_sequence else false;
    const phase: TransitionPhase = if (rolled_back)
        .rolled_back
    else if (rolling_back)
        .rolling_back
    else if (finalized)
        .finalized
    else if (!receiver_accepts_donor_range)
        .prepare
    else if (!bootstrapped)
        .bootstrap_peer
    else if (receiver_delta_sequence < donor_delta_sequence)
        .replay_deltas
    else
        .cutover_ready;

    return .{
        .phase = phase,
        .donor_group_id = donor_group_id,
        .receiver_group_id = receiver_group_id,
        .receiver_accepts_donor_range = receiver_accepts_donor_range,
        .bootstrapped = bootstrapped,
        .replay_required = replay_required,
        .replay_caught_up = replay_caught_up,
        .cutover_ready = phase == .cutover_ready or phase == .finalized,
        .receiver_ready_for_reads = phase == .cutover_ready or phase == .finalized,
        .donor_delta_sequence = donor_delta_sequence,
        .receiver_delta_sequence = receiver_delta_sequence,
    };
}

pub fn receiverParticipant(status: MergeStatus) MergeParticipantStatus {
    return .{
        .role = .receiver,
        .phase = status.phase,
        .donor_group_id = status.donor_group_id,
        .receiver_group_id = status.receiver_group_id,
        .accepts_donor_range = status.receiver_accepts_donor_range,
        .replay_required = status.replay_required,
        .replay_caught_up = status.replay_caught_up,
        .cutover_ready = status.cutover_ready,
    };
}

pub fn donorParticipant(status: MergeStatus) MergeParticipantStatus {
    return .{
        .role = .donor,
        .phase = status.phase,
        .donor_group_id = status.donor_group_id,
        .receiver_group_id = status.receiver_group_id,
        .accepts_donor_range = false,
        .replay_required = status.replay_required,
        .replay_caught_up = status.replay_caught_up,
        .cutover_ready = status.cutover_ready,
    };
}

pub fn validateMirroredMergePair(
    donor: MergeParticipantStatus,
    receiver: MergeParticipantStatus,
) MergePairError!void {
    if (donor.role != .donor or receiver.role != .receiver) return error.InvalidParticipantRole;
    if (donor.donor_group_id != receiver.donor_group_id or donor.receiver_group_id != receiver.receiver_group_id)
        return error.MismatchedPairIds;
    if (receiver.accepts_donor_range != true) return error.ReceiverMustAcceptDonorRange;
    if (donor.accepts_donor_range != false) return error.DonorMustNotAcceptDonorRange;
    if (donor.phase != receiver.phase) return error.MismatchedPhase;
    if (donor.replay_required != receiver.replay_required) return error.MismatchedReplayRequirement;
    if (donor.replay_caught_up != receiver.replay_caught_up) return error.MismatchedReplayCaughtUp;
    if (donor.cutover_ready != receiver.cutover_ready) return error.MismatchedCutoverReadiness;
}

test "derive split transition phases" {
    {
        const status = deriveSplitStatus(.prepare, false, 0, 0);
        try std.testing.expectEqual(TransitionPhase.prepare, status.phase);
        try std.testing.expect(!status.destination_ready_for_reads);
    }
    {
        const status = deriveSplitStatus(.splitting, false, 0, 0);
        try std.testing.expectEqual(TransitionPhase.bootstrap_peer, status.phase);
    }
    {
        const status = deriveSplitStatus(.splitting, true, 2, 1);
        try std.testing.expectEqual(TransitionPhase.replay_deltas, status.phase);
        try std.testing.expect(status.replay_required);
        try std.testing.expect(!status.replay_caught_up);
    }
    {
        const status = deriveSplitStatus(.splitting, true, 2, 2);
        try std.testing.expectEqual(TransitionPhase.cutover_ready, status.phase);
        try std.testing.expect(status.destination_ready_for_reads);
    }
    {
        const status = deriveSplitStatus(.none, true, 2, 2);
        try std.testing.expectEqual(TransitionPhase.finalized, status.phase);
    }
}

test "derive merge transition phases" {
    {
        const status = deriveMergeStatus(10, 20, false, false, 0, 0, false, false, false);
        try std.testing.expectEqual(TransitionPhase.prepare, status.phase);
        try std.testing.expect(!status.receiver_accepts_donor_range);
    }
    {
        const status = deriveMergeStatus(10, 20, true, false, 0, 0, false, false, false);
        try std.testing.expectEqual(TransitionPhase.bootstrap_peer, status.phase);
    }
    {
        const status = deriveMergeStatus(10, 20, true, true, 3, 2, false, false, false);
        try std.testing.expectEqual(TransitionPhase.replay_deltas, status.phase);
        try std.testing.expect(status.replay_required);
        try std.testing.expect(!status.replay_caught_up);
    }
    {
        const status = deriveMergeStatus(10, 20, true, true, 3, 3, false, false, false);
        try std.testing.expectEqual(TransitionPhase.cutover_ready, status.phase);
        try std.testing.expect(status.receiver_ready_for_reads);
    }
    {
        const status = deriveMergeStatus(10, 20, true, true, 3, 3, false, true, false);
        try std.testing.expectEqual(TransitionPhase.finalized, status.phase);
    }
    {
        const status = deriveMergeStatus(10, 20, false, false, 3, 0, false, false, true);
        try std.testing.expectEqual(TransitionPhase.rolled_back, status.phase);
        try std.testing.expect(!status.replay_required);
    }
}

test "validate mirrored merge pair" {
    const status = deriveMergeStatus(10, 20, true, true, 3, 3, false, false, false);
    try validateMirroredMergePair(donorParticipant(status), receiverParticipant(status));
}

test "reject mismatched mirrored merge pair" {
    const status = deriveMergeStatus(10, 20, true, true, 3, 2, false, false, false);
    const receiver = receiverParticipant(status);
    var donor = donorParticipant(status);
    donor.replay_caught_up = true;
    try std.testing.expectError(error.MismatchedReplayCaughtUp, validateMirroredMergePair(donor, receiver));
}
