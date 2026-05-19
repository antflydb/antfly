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
const transition_actions = @import("transition_actions.zig");
const transition_state = @import("transition_state.zig");

pub const TransitionDecision = transition_actions.TransitionDecision;
pub const TransitionAction = transition_actions.TransitionAction;

pub const SplitExecutionStateTag = enum {
    awaiting_source_start,
    bootstrapping_destination,
    replay_blocked,
    ready_to_finalize,
    finalizing,
    finalized,
    rolling_back,
    rolled_back,
};

pub const MergeExecutionStateTag = enum {
    awaiting_receiver_acceptance,
    bootstrapping_receiver,
    replay_blocked,
    ready_to_finalize,
    finalizing,
    finalized,
    rolling_back,
    rolled_back,
};

pub const SplitExecutionState = struct {
    tag: SplitExecutionStateTag,
    next_phase: transition_state.TransitionPhase,
    action: TransitionAction,
    observation: transition_state.SplitObservation,

    pub fn actionable(self: SplitExecutionState) bool {
        return self.action != .none;
    }
};

pub const MergeExecutionState = struct {
    tag: MergeExecutionStateTag,
    next_phase: transition_state.TransitionPhase,
    action: TransitionAction,
    observation: transition_state.MergeObservation,

    pub fn actionable(self: MergeExecutionState) bool {
        return self.action != .none;
    }
};

pub const TransitionController = struct {
    pub fn describeSplit(
        record: transition_state.SplitTransitionRecord,
        observation: transition_state.SplitObservation,
    ) SplitExecutionState {
        const tag = splitTag(record, observation);
        const decision = splitDecisionForTag(tag, record, observation);
        return .{
            .tag = tag,
            .next_phase = decision.next_phase,
            .action = decision.action,
            .observation = observation,
        };
    }

    pub fn planSplit(
        record: transition_state.SplitTransitionRecord,
        observation: transition_state.SplitObservation,
    ) TransitionDecision {
        return splitDecisionForTag(splitTag(record, observation), record, observation);
    }

    pub fn describeMerge(
        record: transition_state.MergeTransitionRecord,
        observation: transition_state.MergeObservation,
    ) MergeExecutionState {
        const tag = mergeTag(record, observation);
        const decision = mergeDecisionForTag(tag, record, observation);
        return .{
            .tag = tag,
            .next_phase = decision.next_phase,
            .action = decision.action,
            .observation = observation,
        };
    }

    pub fn planMerge(
        record: transition_state.MergeTransitionRecord,
        observation: transition_state.MergeObservation,
    ) TransitionDecision {
        return mergeDecisionForTag(mergeTag(record, observation), record, observation);
    }

    fn splitTag(
        record: transition_state.SplitTransitionRecord,
        observation: transition_state.SplitObservation,
    ) SplitExecutionStateTag {
        return if (record.rollback_reason != null)
            if (observation.status.phase == .rolled_back) .rolled_back else .rolling_back
        else switch (observation.status.phase) {
            .prepare => .awaiting_source_start,
            .bootstrap_peer => .bootstrapping_destination,
            .replay_deltas => .replay_blocked,
            .cutover_ready => .ready_to_finalize,
            .finalized => .finalized,
            .rolling_back => .rolling_back,
            .rolled_back => if (record.split_key != null) .awaiting_source_start else .rolled_back,
        };
    }

    fn splitDecisionForTag(
        tag: SplitExecutionStateTag,
        record: transition_state.SplitTransitionRecord,
        observation: transition_state.SplitObservation,
    ) TransitionDecision {
        return switch (tag) {
            .awaiting_source_start => .{
                .next_phase = if (observation.status.phase == .rolled_back) .prepare else .bootstrap_peer,
                .action = if (observation.status.phase == .rolled_back)
                    if (record.split_key) |split_key| .{
                        .prepare_split_source = .{
                            .transition_id = record.transition_id,
                            .source_group_id = record.source_group_id,
                            .destination_group_id = record.destination_group_id,
                            .split_key = split_key,
                            .source_range_end = record.source_range_end,
                        },
                    } else .none
                else .{
                    .start_split_source = .{
                        .transition_id = record.transition_id,
                        .source_group_id = record.source_group_id,
                        .destination_group_id = record.destination_group_id,
                    },
                },
            },
            .bootstrapping_destination => .{
                .next_phase = .bootstrap_peer,
                .action = .{
                    .bootstrap_split_destination = .{
                        .transition_id = record.transition_id,
                        .source_group_id = record.source_group_id,
                        .destination_group_id = record.destination_group_id,
                    },
                },
            },
            .replay_blocked => .{
                .next_phase = .replay_deltas,
                .action = .{
                    .catch_up_split_destination = .{
                        .transition_id = record.transition_id,
                        .source_group_id = record.source_group_id,
                        .destination_group_id = record.destination_group_id,
                    },
                },
            },
            .ready_to_finalize, .finalizing => .{
                .next_phase = .finalizing,
                .action = .{
                    .finalize_split_source = .{
                        .transition_id = record.transition_id,
                        .source_group_id = record.source_group_id,
                        .destination_group_id = record.destination_group_id,
                    },
                },
            },
            .finalized => .{
                .next_phase = .finalized,
                .action = .none,
            },
            .rolling_back => .{
                .next_phase = if (observation.status.phase == .rolled_back) .rolled_back else .rolling_back,
                .action = if (observation.status.phase == .rolled_back) .none else .{
                    .rollback_split = .{
                        .transition_id = record.transition_id,
                        .source_group_id = record.source_group_id,
                        .destination_group_id = record.destination_group_id,
                    },
                },
            },
            .rolled_back => .{
                .next_phase = .rolled_back,
                .action = .none,
            },
        };
    }

    fn mergeTag(
        record: transition_state.MergeTransitionRecord,
        observation: transition_state.MergeObservation,
    ) MergeExecutionStateTag {
        return if (record.rollback_reason != null)
            if (observation.receiver.phase == .rolled_back) .rolled_back else .rolling_back
        else switch (observation.receiver.phase) {
            .prepare => .awaiting_receiver_acceptance,
            .bootstrap_peer => .bootstrapping_receiver,
            .replay_deltas => .replay_blocked,
            .cutover_ready => .ready_to_finalize,
            .finalized => .finalized,
            .rolling_back => .rolling_back,
            .rolled_back => .awaiting_receiver_acceptance,
        };
    }

    fn mergeDecisionForTag(
        tag: MergeExecutionStateTag,
        record: transition_state.MergeTransitionRecord,
        observation: transition_state.MergeObservation,
    ) TransitionDecision {
        return switch (tag) {
            .awaiting_receiver_acceptance => .{
                .next_phase = .bootstrap_peer,
                .action = .{
                    .accept_merge_receiver = .{
                        .transition_id = record.transition_id,
                        .donor_group_id = record.donor_group_id,
                        .receiver_group_id = record.receiver_group_id,
                    },
                },
            },
            .bootstrapping_receiver, .replay_blocked => .{
                .next_phase = if (observation.receiver.phase == .bootstrap_peer) .bootstrap_peer else .replay_deltas,
                .action = .{
                    .catch_up_merge_receiver = .{
                        .transition_id = record.transition_id,
                        .donor_group_id = record.donor_group_id,
                        .receiver_group_id = record.receiver_group_id,
                    },
                },
            },
            .ready_to_finalize, .finalizing => .{
                .next_phase = .finalizing,
                .action = .{
                    .finalize_merge = .{
                        .transition_id = record.transition_id,
                        .donor_group_id = record.donor_group_id,
                        .receiver_group_id = record.receiver_group_id,
                    },
                },
            },
            .finalized => .{
                .next_phase = .finalized,
                .action = .none,
            },
            .rolling_back => .{
                .next_phase = if (observation.receiver.phase == .rolled_back) .rolled_back else .rolling_back,
                .action = if (observation.receiver.phase == .rolled_back) .none else .{
                    .rollback_merge = .{
                        .transition_id = record.transition_id,
                        .donor_group_id = record.donor_group_id,
                        .receiver_group_id = record.receiver_group_id,
                    },
                },
            },
            .rolled_back => .{
                .next_phase = .rolled_back,
                .action = .none,
            },
        };
    }
};

test "transition controller plans split start bootstrap and finalize actions" {
    const split_record = transition_state.SplitTransitionRecord{
        .transition_id = 10,
        .source_group_id = 41,
        .destination_group_id = 42,
    };

    const start = TransitionController.planSplit(split_record, .{
        .status = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
    });
    try std.testing.expectEqual(transition_state.TransitionPhase.bootstrap_peer, start.next_phase);
    try std.testing.expect(start.action == .start_split_source);

    const bootstrap = TransitionController.planSplit(split_record, .{
        .status = .{
            .phase = .bootstrap_peer,
            .source_split_phase = .splitting,
            .bootstrapped = false,
            .replay_required = true,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
    });
    try std.testing.expectEqual(transition_state.TransitionPhase.bootstrap_peer, bootstrap.next_phase);
    try std.testing.expect(bootstrap.action == .bootstrap_split_destination);

    const finalize = TransitionController.planSplit(split_record, .{
        .status = .{
            .phase = .cutover_ready,
            .source_split_phase = .finalizing,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .destination_ready_for_reads = true,
            .source_delta_sequence = 4,
            .dest_delta_sequence = 4,
        },
    });
    try std.testing.expectEqual(transition_state.TransitionPhase.finalizing, finalize.next_phase);
    try std.testing.expect(finalize.action == .finalize_split_source);
}

test "transition controller plans merge accept catch-up and rollback actions" {
    const merge_record = transition_state.MergeTransitionRecord{
        .transition_id = 11,
        .donor_group_id = 51,
        .receiver_group_id = 52,
    };

    const accept = TransitionController.planMerge(merge_record, .{
        .donor = .{
            .phase = .prepare,
            .donor_group_id = 51,
            .receiver_group_id = 52,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        .receiver = .{
            .phase = .prepare,
            .donor_group_id = 51,
            .receiver_group_id = 52,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
    });
    try std.testing.expectEqual(transition_state.TransitionPhase.bootstrap_peer, accept.next_phase);
    try std.testing.expect(accept.action == .accept_merge_receiver);

    const catch_up = TransitionController.planMerge(merge_record, .{
        .donor = .{
            .phase = .replay_deltas,
            .donor_group_id = 51,
            .receiver_group_id = 52,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 3,
            .receiver_delta_sequence = 2,
        },
        .receiver = .{
            .phase = .replay_deltas,
            .donor_group_id = 51,
            .receiver_group_id = 52,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 3,
            .receiver_delta_sequence = 2,
        },
    });
    try std.testing.expectEqual(transition_state.TransitionPhase.replay_deltas, catch_up.next_phase);
    try std.testing.expect(catch_up.action == .catch_up_merge_receiver);

    const rollback_record = transition_state.MergeTransitionRecord{
        .transition_id = 11,
        .donor_group_id = 51,
        .receiver_group_id = 52,
        .rollback_reason = "operator abort",
    };
    const rollback = TransitionController.planMerge(rollback_record, .{
        .donor = .{
            .phase = .cutover_ready,
            .donor_group_id = 51,
            .receiver_group_id = 52,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .receiver_ready_for_reads = true,
            .donor_delta_sequence = 4,
            .receiver_delta_sequence = 4,
        },
        .receiver = .{
            .phase = .cutover_ready,
            .donor_group_id = 51,
            .receiver_group_id = 52,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .receiver_ready_for_reads = true,
            .donor_delta_sequence = 4,
            .receiver_delta_sequence = 4,
        },
    });
    try std.testing.expectEqual(transition_state.TransitionPhase.rolling_back, rollback.next_phase);
    try std.testing.expect(rollback.action == .rollback_merge);
}

test "transition controller describes split and merge execution states" {
    const split = TransitionController.describeSplit(.{
        .transition_id = 1,
        .source_group_id = 10,
        .destination_group_id = 11,
    }, .{
        .status = .{
            .phase = .replay_deltas,
            .source_split_phase = .splitting,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 3,
            .dest_delta_sequence = 2,
        },
    });
    try std.testing.expectEqual(SplitExecutionStateTag.replay_blocked, split.tag);
    try std.testing.expect(split.actionable());
    try std.testing.expect(split.action == .catch_up_split_destination);

    const merge = TransitionController.describeMerge(.{
        .transition_id = 2,
        .donor_group_id = 20,
        .receiver_group_id = 21,
    }, .{
        .donor = .{
            .phase = .cutover_ready,
            .donor_group_id = 20,
            .receiver_group_id = 21,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .receiver_ready_for_reads = true,
            .donor_delta_sequence = 4,
            .receiver_delta_sequence = 4,
        },
        .receiver = .{
            .phase = .cutover_ready,
            .donor_group_id = 20,
            .receiver_group_id = 21,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .receiver_ready_for_reads = true,
            .donor_delta_sequence = 4,
            .receiver_delta_sequence = 4,
        },
    });
    try std.testing.expectEqual(MergeExecutionStateTag.ready_to_finalize, merge.tag);
    try std.testing.expect(merge.actionable());
    try std.testing.expect(merge.action == .finalize_merge);
}
