// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const trace_mod = @import("trace.zig");
const core = @import("../core/mod.zig");

const canonical_fixture = @embedFile("testdata/canonical_leader_proposal.json");
const differential_fixture = @embedFile("testdata/differential_campaign_proposal.json");
const differential_pre_vote_fixture = @embedFile("testdata/differential_pre_vote_campaign_proposal.json");
const differential_pre_vote_check_quorum_lease_protection_fixture = @embedFile("testdata/differential_pre_vote_check_quorum_lease_protection.json");
const differential_pre_vote_migration_fixture = @embedFile("testdata/differential_pre_vote_migration_election.json");
const leader_transfer_fixture = @embedFile("testdata/differential_leader_transfer.json");
const leader_transfer_slow_fixture = @embedFile("testdata/differential_leader_transfer_slow_follower.json");
const check_quorum_leader_transfer_slow_fixture = @embedFile("testdata/differential_check_quorum_leader_transfer_slow_follower.json");
const leader_transfer_timeout_fixture = @embedFile("testdata/differential_leader_transfer_timeout.json");
const check_quorum_leader_transfer_timeout_fixture = @embedFile("testdata/differential_check_quorum_leader_transfer_timeout.json");
const leader_transfer_from_follower_fixture = @embedFile("testdata/differential_leader_transfer_from_follower.json");
const leader_transfer_replace_pending_fixture = @embedFile("testdata/differential_leader_transfer_replace_pending.json");
const check_quorum_leader_transfer_replace_pending_fixture = @embedFile("testdata/differential_check_quorum_leader_transfer_replace_pending.json");
const leader_transfer_same_pending_fixture = @embedFile("testdata/differential_leader_transfer_same_pending_timeout.json");
const leader_transfer_higher_term_fixture = @embedFile("testdata/differential_leader_transfer_higher_term_abort.json");
const leader_transfer_learner_target_fixture = @embedFile("testdata/differential_leader_transfer_learner_target.json");
const check_quorum_transfer_joint_reconfig_fixture = @embedFile("testdata/differential_check_quorum_transfer_joint_reconfig.json");
const async_check_quorum_transfer_joint_reconfig_fixture = @embedFile("testdata/differential_async_check_quorum_transfer_joint_reconfig.json");
const check_quorum_step_down_fixture = @embedFile("testdata/differential_check_quorum_leader_step_down.json");
const check_quorum_follower_lease_protection_fixture = @embedFile("testdata/differential_check_quorum_follower_lease_protection.json");
const check_quorum_higher_term_disruption_fixture = @embedFile("testdata/differential_check_quorum_higher_term_disruption.json");
const check_quorum_repeated_higher_term_disruption_fixture = @embedFile("testdata/differential_check_quorum_repeated_higher_term_disruption.json");
const check_quorum_snapshot_transfer_fixture = @embedFile("testdata/differential_check_quorum_snapshot_transfer.json");
const stress_joint_restart_churn_fixture = @embedFile("testdata/differential_stress_joint_restart_churn.json");
const stress_seeded_restart_config_churn_fixture = @embedFile("testdata/differential_stress_seeded_restart_config_churn.json");
const stress_seeded_transfer_churn_fixture = @embedFile("testdata/differential_stress_seeded_transfer_churn.json");
const stress_seeded_lease_transfer_churn_fixture = @embedFile("testdata/differential_stress_seeded_lease_transfer_churn.json");
const stress_seeded_snapshot_restore_churn_fixture = @embedFile("testdata/differential_stress_seeded_snapshot_restore_churn.json");
const stress_seeded_async_churn_fixture = @embedFile("testdata/differential_stress_seeded_async_churn.json");
const stress_seeded_async_lease_churn_fixture = @embedFile("testdata/differential_stress_seeded_async_lease_churn.json");
const stress_seeded_async_restart_snapshot_churn_fixture = @embedFile("testdata/differential_stress_seeded_async_restart_snapshot_churn.json");
const stress_seeded_async_lease_restart_snapshot_churn_fixture = @embedFile("testdata/differential_stress_seeded_async_lease_restart_snapshot_churn.json");
const read_index_fixture = @embedFile("testdata/differential_read_index.json");
const follower_read_index_fixture = @embedFile("testdata/differential_follower_read_index.json");
const read_index_lease_based_fixture = @embedFile("testdata/differential_read_index_lease_based.json");
const read_index_lease_based_joint_fixture = @embedFile("testdata/differential_read_index_lease_based_joint.json");
const read_index_lease_based_transfer_fixture = @embedFile("testdata/differential_read_index_lease_based_transfer.json");
const read_index_lease_based_restart_fixture = @embedFile("testdata/differential_read_index_lease_based_restart.json");
const read_index_lease_based_reelection_fixture = @embedFile("testdata/differential_read_index_lease_based_reelection.json");
const read_index_lease_based_auto_reelection_fixture = @embedFile("testdata/differential_read_index_lease_based_auto_reelection.json");
const read_index_lease_based_expiry_fixture = @embedFile("testdata/differential_read_index_lease_based_expiry_and_reelection.json");
const pre_vote_read_index_lease_based_auto_reelection_fixture = @embedFile("testdata/differential_pre_vote_read_index_lease_based_auto_reelection.json");
const async_read_index_lease_based_fixture = @embedFile("testdata/differential_async_read_index_lease_based.json");
const async_read_index_lease_based_joint_fixture = @embedFile("testdata/differential_async_read_index_lease_based_joint.json");
const async_read_index_lease_based_transfer_fixture = @embedFile("testdata/differential_async_read_index_lease_based_transfer.json");
const async_read_index_lease_based_restart_fixture = @embedFile("testdata/differential_async_read_index_lease_based_restart.json");
const async_read_index_lease_based_reelection_fixture = @embedFile("testdata/differential_async_read_index_lease_based_reelection.json");
const async_read_index_lease_based_expiry_fixture = @embedFile("testdata/differential_async_read_index_lease_based_expiry_and_reelection.json");
const async_pre_vote_read_index_lease_based_auto_reelection_fixture = @embedFile("testdata/differential_async_pre_vote_read_index_lease_based_auto_reelection.json");
const async_pre_vote_read_index_lease_based_expiry_fixture = @embedFile("testdata/differential_async_pre_vote_read_index_lease_based_expiry_and_reelection.json");
const follower_proposal_fixture = @embedFile("testdata/differential_follower_proposal.json");
const follower_conf_change_fixture = @embedFile("testdata/differential_follower_conf_change.json");
const disable_proposal_forwarding_fixture = @embedFile("testdata/differential_disable_proposal_forwarding_leader_proposal.json");
const forget_leader_fixture = @embedFile("testdata/differential_forget_leader.json");
const forget_leader_lease_based_fixture = @embedFile("testdata/differential_forget_leader_lease_based.json");
const max_inflight_gating_fixture = @embedFile("testdata/differential_max_inflight_gating.json");
const async_storage_writes_fixture = @embedFile("testdata/differential_async_storage_writes.json");
const max_size_per_msg_batching_fixture = @embedFile("testdata/differential_max_size_per_msg_batching.json");
const max_uncommitted_entries_size_fixture = @embedFile("testdata/differential_max_uncommitted_entries_size.json");
const read_index_multi_fixture = @embedFile("testdata/differential_read_index_multi.json");
const check_quorum_read_index_multi_fixture = @embedFile("testdata/differential_check_quorum_read_index_multi.json");
const read_index_joint_fixture = @embedFile("testdata/differential_read_index_joint.json");
const read_index_transfer_fixture = @embedFile("testdata/differential_read_index_transfer.json");
const check_quorum_read_index_transfer_fixture = @embedFile("testdata/differential_check_quorum_read_index_transfer.json");
const read_index_restart_fixture = @embedFile("testdata/differential_read_index_restart_clear_pending.json");
const check_quorum_read_index_restart_fixture = @embedFile("testdata/differential_check_quorum_read_index_restart_clear_pending.json");
const read_index_reelection_fixture = @embedFile("testdata/differential_read_index_reelection_clear_pending.json");
const joint_auto_leave_fixture = @embedFile("testdata/differential_joint_auto_leave.json");
const joint_explicit_leave_fixture = @embedFile("testdata/differential_joint_explicit_leave.json");
const check_quorum_joint_explicit_leave_fixture = @embedFile("testdata/differential_check_quorum_joint_explicit_leave.json");
const check_quorum_joint_incoming_voter_replacement_fixture = @embedFile("testdata/differential_check_quorum_joint_incoming_voter_replacement.json");
const pre_vote_check_quorum_joint_incoming_voter_replacement_fixture = @embedFile("testdata/differential_pre_vote_check_quorum_joint_incoming_voter_replacement.json");
const check_quorum_partition_joint_churn_fixture = @embedFile("testdata/differential_check_quorum_partition_joint_churn.json");
const joint_multi_add_fixture = @embedFile("testdata/differential_joint_multi_add_explicit.json");
const joint_replace_voter_fixture = @embedFile("testdata/differential_joint_replace_voter.json");
const joint_demote_voter_fixture = @embedFile("testdata/differential_joint_demote_voter.json");
const joint_mixed_change_fixture = @embedFile("testdata/differential_joint_mixed_change.json");
const check_quorum_joint_mixed_change_fixture = @embedFile("testdata/differential_check_quorum_joint_mixed_change.json");
const check_quorum_joint_demoted_outgoing_election_fixture = @embedFile("testdata/differential_check_quorum_joint_demoted_outgoing_election.json");
const pre_vote_check_quorum_joint_demoted_outgoing_election_fixture = @embedFile("testdata/differential_pre_vote_check_quorum_joint_demoted_outgoing_election.json");
const joint_chained_mixed_fixture = @embedFile("testdata/differential_joint_chained_mixed_change.json");
const joint_idempotent_mixed_fixture = @embedFile("testdata/differential_joint_idempotent_mixed.json");
const joint_learners_next_churn_fixture = @embedFile("testdata/differential_joint_learners_next_churn.json");
const step_down_on_removal_fixture = @embedFile("testdata/differential_step_down_on_removal.json");
const learner_catchup_fixture = @embedFile("testdata/differential_learner_catchup.json");
const restart_learner_readd_fixture = @embedFile("testdata/differential_restart_learner_readd_replay.json");
const restart_joint_fixture = @embedFile("testdata/differential_restart_joint_replay.json");
const check_quorum_restart_joint_fixture = @embedFile("testdata/differential_check_quorum_restart_joint_replay.json");
const restart_joint_mixed_fixture = @embedFile("testdata/differential_restart_joint_mixed_replay.json");
const restart_joint_partial_fixture = @embedFile("testdata/differential_restart_joint_partial_replay.json");
const restart_joint_snapshot_fixture = @embedFile("testdata/differential_restart_joint_snapshot_replay.json");
const check_quorum_restart_joint_snapshot_progress_fixture = @embedFile("testdata/differential_check_quorum_restart_joint_snapshot_progress.json");
const restart_joint_leader_churn_fixture = @embedFile("testdata/differential_restart_joint_leader_churn_replay.json");
const pre_vote_check_quorum_restart_joint_snapshot_progress_fixture = @embedFile("testdata/differential_pre_vote_check_quorum_restart_joint_snapshot_progress.json");
const restart_joint_readd_fixture = @embedFile("testdata/differential_restart_joint_readd_removed_replay.json");
const restart_joint_demote_promote_fixture = @embedFile("testdata/differential_restart_joint_demote_promote_replay.json");
const catchup_fixture = @embedFile("testdata/differential_follower_catchup.json");
const split_vote_fixture = @embedFile("testdata/differential_split_vote_retry.json");
const conflict_fixture = @embedFile("testdata/differential_conflict_backtrack.json");
const restart_fixture = @embedFile("testdata/differential_restart_replay.json");
const check_quorum_restart_fixture = @embedFile("testdata/differential_check_quorum_restart_replay.json");
const restart_applied_fixture = @embedFile("testdata/differential_restart_applied_replay.json");
const check_quorum_restart_applied_fixture = @embedFile("testdata/differential_check_quorum_restart_applied_replay.json");
const async_restart_applied_fixture = @embedFile("testdata/differential_async_restart_applied_replay.json");
const check_quorum_restart_joint_mixed_fixture = @embedFile("testdata/differential_check_quorum_restart_joint_mixed_replay.json");
const reject_backoff_fixture = @embedFile("testdata/differential_reject_backoff.json");
const snapshot_compaction_fixture = @embedFile("testdata/differential_snapshot_compaction_replay.json");
const snapshot_joint_transport_fixture = @embedFile("testdata/differential_snapshot_joint_transport.json");
const snapshot_joint_reelection_fixture = @embedFile("testdata/differential_snapshot_joint_reelection_overlap.json");
const snapshot_readd_removed_voter_fixture = @embedFile("testdata/differential_snapshot_readd_removed_voter.json");
const snapshot_success_fixture = @embedFile("testdata/differential_snapshot_success.json");
const snapshot_failure_fixture = @embedFile("testdata/differential_snapshot_failure.json");
const snapshot_abort_fixture = @embedFile("testdata/differential_snapshot_abort.json");
const snapshot_transport_fixture = @embedFile("testdata/differential_snapshot_transport.json");
const snapshot_retry_fixture = @embedFile("testdata/differential_snapshot_retry.json");
const membership_change_fixture = @embedFile("testdata/differential_membership_change.json");

test "trace recorder captures canonical election and proposal flow" {
    var trace = try trace_mod.recordCanonicalLeaderProposal(std.testing.allocator);
    defer trace.deinit();

    const steps = trace.stepsSlice();
    try std.testing.expectEqual(@as(usize, 4), steps.len);
    try std.testing.expectEqual(core.types.StateRole.leader, steps[1].nodes[0].role);
    try std.testing.expectEqual(@as(core.types.Index, 1), steps[1].nodes[0].commit_index);
    try std.testing.expectEqual(@as(usize, 2), steps[0].messages.len);
    try std.testing.expectEqual(@as(usize, 2), steps[2].messages.len);
    try std.testing.expectEqual(@as(core.types.Index, 2), steps[3].nodes[0].commit_index);
    try std.testing.expectEqual(@as(usize, 3), steps[3].committed.len);
    try std.testing.expectEqual(@as(core.types.NodeId, 1), steps[3].committed[0].node_id);
    try std.testing.expectEqual(@as(usize, 2), steps[3].committed[0].count);

    const rendered = try trace.render(std.testing.allocator);
    defer std.testing.allocator.free(rendered);

    try std.testing.expect(std.mem.indexOf(u8, rendered, "step 0: tick node=1 count=3") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "node 1 role=leader") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "msg append_entries 1->2") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "committed node=1 count=2 first=1 last=2") != null);
}

test "canonical trace json matches checked-in fixture" {
    var trace = try trace_mod.recordCanonicalLeaderProposal(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(canonical_fixture, actual);
}

test "differential trace json matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCampaignProposal(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(differential_fixture, actual);
}

test "pre-vote differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialPreVoteCampaignProposal(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(differential_pre_vote_fixture, actual);
}

test "pre-vote check-quorum lease-protection differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialPreVoteCheckQuorumLeaseProtection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(differential_pre_vote_check_quorum_lease_protection_fixture, actual);
}

test "pre-vote migration differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialPreVoteMigrationElection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(differential_pre_vote_migration_fixture, actual);
}

test "leader-transfer differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransfer(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_fixture, actual);
}

test "leader-transfer slow-follower differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransferSlowFollower(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_slow_fixture, actual);
}

test "check-quorum leader-transfer slow-follower differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumLeaderTransferSlowFollower(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_leader_transfer_slow_fixture, actual);
}

test "leader-transfer timeout differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransferTimeout(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_timeout_fixture, actual);
}

test "check-quorum leader-transfer timeout differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumLeaderTransferTimeout(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_leader_transfer_timeout_fixture, actual);
}

test "leader-transfer from-follower differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransferFromFollower(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_from_follower_fixture, actual);
}

test "leader-transfer replace-pending differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransferReplacePending(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_replace_pending_fixture, actual);
}

test "check-quorum leader-transfer replace-pending differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumLeaderTransferReplacePending(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_leader_transfer_replace_pending_fixture, actual);
}

test "leader-transfer same-pending timeout differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransferSamePendingTimeout(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_same_pending_fixture, actual);
}

test "leader-transfer higher-term abort differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransferHigherTermAbort(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_higher_term_fixture, actual);
}

test "leader-transfer learner-target differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLeaderTransferLearnerTarget(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(leader_transfer_learner_target_fixture, actual);
}

test "check-quorum transfer-joint-reconfig differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumTransferJointReconfig(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_transfer_joint_reconfig_fixture, actual);
}

test "async check-quorum transfer-joint-reconfig differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncCheckQuorumTransferJointReconfig(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_check_quorum_transfer_joint_reconfig_fixture, actual);
}

test "check-quorum leader-stepdown differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumLeaderStepDown(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_step_down_fixture, actual);
}

test "check-quorum follower lease-protection differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumFollowerLeaseProtection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_follower_lease_protection_fixture, actual);
}

test "check-quorum higher-term disruption differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumHigherTermDisruption(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_higher_term_disruption_fixture, actual);
}

test "check-quorum repeated higher-term disruption differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumRepeatedHigherTermDisruption(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_repeated_higher_term_disruption_fixture, actual);
}

test "check-quorum snapshot-transfer differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumSnapshotTransfer(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_snapshot_transfer_fixture, actual);
}

test "stress joint-restart churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressJointRestartChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_joint_restart_churn_fixture, actual);
}

test "stress seeded restart-config churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededRestartConfigChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_restart_config_churn_fixture, actual);
}

test "stress seeded transfer churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededTransferChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_transfer_churn_fixture, actual);
}

test "stress seeded lease-transfer churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededLeaseTransferChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_lease_transfer_churn_fixture, actual);
}

test "stress seeded snapshot-restore churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededSnapshotRestoreChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_snapshot_restore_churn_fixture, actual);
}

test "stress seeded async churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededAsyncChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_async_churn_fixture, actual);
}

test "stress seeded async lease churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededAsyncLeaseChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_async_lease_churn_fixture, actual);
}

test "stress seeded async restart-snapshot churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededAsyncRestartSnapshotChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_async_restart_snapshot_churn_fixture, actual);
}

test "stress seeded async lease restart-snapshot churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStressSeededAsyncLeaseRestartSnapshotChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(stress_seeded_async_lease_restart_snapshot_churn_fixture, actual);
}

test "read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndex(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_fixture, actual);
}

test "follower read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialFollowerReadIndex(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(follower_read_index_fixture, actual);
}

test "lease-based read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexLeaseBased(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_lease_based_fixture, actual);
}

test "lease-based joint-config read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexLeaseBasedJointConfig(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_lease_based_joint_fixture, actual);
}

test "lease-based leader-transfer read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexLeaseBasedDuringLeaderTransfer(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_lease_based_transfer_fixture, actual);
}

test "lease-based restart read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexLeaseBasedRestart(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_lease_based_restart_fixture, actual);
}

test "lease-based reelection read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexLeaseBasedReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_lease_based_reelection_fixture, actual);
}

test "lease-based automatic reelection read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexLeaseBasedAutomaticReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_lease_based_auto_reelection_fixture, actual);
}

test "lease-based expiry and reelection differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexLeaseBasedExpiryAndReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_lease_based_expiry_fixture, actual);
}

test "pre-vote lease-based automatic reelection read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialPreVoteReadIndexLeaseBasedAutomaticReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(pre_vote_read_index_lease_based_auto_reelection_fixture, actual);
}

test "async lease-based read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncReadIndexLeaseBased(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_read_index_lease_based_fixture, actual);
}

test "async lease-based joint read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncReadIndexLeaseBasedJointConfig(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_read_index_lease_based_joint_fixture, actual);
}

test "async lease-based transfer read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncReadIndexLeaseBasedTransfer(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_read_index_lease_based_transfer_fixture, actual);
}

test "async lease-based restart read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncReadIndexLeaseBasedRestart(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_read_index_lease_based_restart_fixture, actual);
}

test "async lease-based reelection read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncReadIndexLeaseBasedReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_read_index_lease_based_reelection_fixture, actual);
}

test "async lease-based expiry read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncReadIndexLeaseBasedExpiryAndReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_read_index_lease_based_expiry_fixture, actual);
}

test "async pre-vote lease-based automatic reelection read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncPreVoteReadIndexLeaseBasedAutomaticReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_pre_vote_read_index_lease_based_auto_reelection_fixture, actual);
}

test "async pre-vote lease-based expiry read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncPreVoteReadIndexLeaseBasedExpiryAndReelection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_pre_vote_read_index_lease_based_expiry_fixture, actual);
}

test "follower proposal differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialFollowerProposal(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(follower_proposal_fixture, actual);
}

test "follower conf-change differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialFollowerConfChange(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(follower_conf_change_fixture, actual);
}

test "disable proposal forwarding differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialDisableProposalForwardingLeaderProposal(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(disable_proposal_forwarding_fixture, actual);
}

test "forget leader differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialForgetLeader(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(forget_leader_fixture, actual);
}

test "lease-based forget leader differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialForgetLeaderLeaseBased(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(forget_leader_lease_based_fixture, actual);
}

test "max-inflight gating differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialMaxInflightGating(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(max_inflight_gating_fixture, actual);
}

test "async-storage-writes differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncStorageWrites(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_storage_writes_fixture, actual);
}

test "max-size-per-msg batching differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialMaxSizePerMsgBatching(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(max_size_per_msg_batching_fixture, actual);
}

test "max-uncommitted-entries-size differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialMaxUncommittedEntriesSize(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(max_uncommitted_entries_size_fixture, actual);
}

test "multi-pending read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexMultiPending(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_multi_fixture, actual);
}

test "check-quorum multi-pending read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumReadIndexMultiPending(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_read_index_multi_fixture, actual);
}

test "joint-config read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexJointConfig(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_joint_fixture, actual);
}

test "leader-transfer read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexDuringLeaderTransfer(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_transfer_fixture, actual);
}

test "check-quorum leader-transfer read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumReadIndexDuringLeaderTransfer(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_read_index_transfer_fixture, actual);
}

test "restart clears pending read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexRestartClearsPending(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_restart_fixture, actual);
}

test "check-quorum restart clears pending read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumReadIndexRestartClearsPending(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_read_index_restart_fixture, actual);
}

test "re-election clears pending read-index differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialReadIndexReelectionClearsPending(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(read_index_reelection_fixture, actual);
}

test "joint auto-leave differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointAutoLeave(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_auto_leave_fixture, actual);
}

test "joint explicit leave differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointExplicitLeave(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_explicit_leave_fixture, actual);
}

test "check-quorum joint explicit leave differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumJointExplicitLeave(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_joint_explicit_leave_fixture, actual);
}

test "check-quorum joint incoming-voter replacement differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumJointIncomingVoterReplacement(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_joint_incoming_voter_replacement_fixture, actual);
}

test "pre-vote check-quorum joint incoming-voter replacement differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialPreVoteCheckQuorumJointIncomingVoterReplacement(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(pre_vote_check_quorum_joint_incoming_voter_replacement_fixture, actual);
}

test "check-quorum partition joint-churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumPartitionJointChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_partition_joint_churn_fixture, actual);
}

test "joint multi-add differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointMultiAddExplicit(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_multi_add_fixture, actual);
}

test "joint replace-voter differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointReplaceVoter(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_replace_voter_fixture, actual);
}

test "joint demote-voter differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointDemoteVoter(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_demote_voter_fixture, actual);
}

test "joint mixed-change differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointMixedChange(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_mixed_change_fixture, actual);
}

test "check-quorum joint mixed-change differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumJointMixedChange(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_joint_mixed_change_fixture, actual);
}

test "check-quorum joint demoted-outgoing election differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumJointDemotedOutgoingElection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_joint_demoted_outgoing_election_fixture, actual);
}

test "pre-vote check-quorum joint demoted-outgoing election differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialPreVoteCheckQuorumJointDemotedOutgoingElection(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(pre_vote_check_quorum_joint_demoted_outgoing_election_fixture, actual);
}

test "joint chained mixed-change differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointChainedMixedChange(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_chained_mixed_fixture, actual);
}

test "joint idempotent mixed-change differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointIdempotentMixed(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_idempotent_mixed_fixture, actual);
}

test "joint learners-next churn differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialJointLearnersNextChurn(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(joint_learners_next_churn_fixture, actual);
}

test "step-down-on-removal differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialStepDownOnRemoval(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(step_down_on_removal_fixture, actual);
}

test "learner catch-up differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialLearnerCatchup(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(learner_catchup_fixture, actual);
}

test "restart learner readd differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartLearnerReaddReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_learner_readd_fixture, actual);
}

test "restart joint replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartJointReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_joint_fixture, actual);
}

test "check-quorum restart joint replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumRestartJointReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_restart_joint_fixture, actual);
}

test "restart mixed joint replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartJointMixedReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_joint_mixed_fixture, actual);
}

test "check-quorum restart mixed joint replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumRestartJointMixedReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_restart_joint_mixed_fixture, actual);
}

test "restart partial joint replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartJointPartialReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_joint_partial_fixture, actual);
}

test "restart joint snapshot replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartJointSnapshotReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_joint_snapshot_fixture, actual);
}

test "check-quorum restart joint snapshot progress differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumRestartJointSnapshotProgress(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_restart_joint_snapshot_progress_fixture, actual);
}

test "restart joint leader-churn replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartJointLeaderChurnReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_joint_leader_churn_fixture, actual);
}

test "pre-vote check-quorum restart joint snapshot-progress differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialPreVoteCheckQuorumRestartJointSnapshotProgress(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(pre_vote_check_quorum_restart_joint_snapshot_progress_fixture, actual);
}

test "restart joint re-add removed replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartJointReaddRemovedReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_joint_readd_fixture, actual);
}

test "restart joint demote-promote replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartJointDemotePromoteReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_joint_demote_promote_fixture, actual);
}

test "follower catch-up differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialFollowerCatchup(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(catchup_fixture, actual);
}

test "split-vote retry differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSplitVoteRetry(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(split_vote_fixture, actual);
}

test "conflicting-log backtrack differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialConflictBacktrack(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(conflict_fixture, actual);
}

test "restart replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_fixture, actual);
}

test "check-quorum restart replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumRestartReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_restart_fixture, actual);
}

test "restart applied replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRestartAppliedReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(restart_applied_fixture, actual);
}

test "check-quorum restart applied replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialCheckQuorumRestartAppliedReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(check_quorum_restart_applied_fixture, actual);
}

test "async restart applied replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialAsyncRestartAppliedReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(async_restart_applied_fixture, actual);
}

test "reject backoff differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialRejectBackoff(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(reject_backoff_fixture, actual);
}

test "snapshot compaction replay differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotCompactionReplay(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_compaction_fixture, actual);
}

test "snapshot joint-transport differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotJointTransport(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_joint_transport_fixture, actual);
}

test "snapshot joint-reelection differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotJointReelectionOverlap(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_joint_reelection_fixture, actual);
}

test "snapshot readd removed voter differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotReaddRemovedVoter(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_readd_removed_voter_fixture, actual);
}

test "snapshot transport differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotTransport(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_transport_fixture, actual);
}

test "snapshot success differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotSuccess(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_success_fixture, actual);
}

test "snapshot failure differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotFailure(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_failure_fixture, actual);
}

test "snapshot abort differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotAbort(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_abort_fixture, actual);
}

test "snapshot retry differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialSnapshotRetry(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(snapshot_retry_fixture, actual);
}

test "membership change differential trace matches checked-in fixture" {
    var trace = try trace_mod.recordDifferentialMembershipChange(std.testing.allocator);
    defer trace.deinit();

    const actual = try trace.toJson(std.testing.allocator);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(membership_change_fixture, actual);
}

test "seeded differential trace generation is deterministic" {
    var first = try trace_mod.recordSeededDifferentialTrace(std.testing.allocator, .{
        .seed = 7,
        .steps = 12,
        .check_quorum = true,
        .pre_vote = true,
    });
    defer first.deinit();

    var second = try trace_mod.recordSeededDifferentialTrace(std.testing.allocator, .{
        .seed = 7,
        .steps = 12,
        .check_quorum = true,
        .pre_vote = true,
    });
    defer second.deinit();

    const first_json = try first.toJson(std.testing.allocator);
    defer std.testing.allocator.free(first_json);
    const second_json = try second.toJson(std.testing.allocator);
    defer std.testing.allocator.free(second_json);

    try std.testing.expectEqualStrings(first_json, second_json);
}

test "stress seeded differential trace generation is deterministic" {
    var first = try trace_mod.recordSeededDifferentialTrace(std.testing.allocator, .{
        .seed = 11,
        .steps = 12,
        .check_quorum = true,
        .pre_vote = true,
        .profile = .stress,
    });
    defer first.deinit();

    var second = try trace_mod.recordSeededDifferentialTrace(std.testing.allocator, .{
        .seed = 11,
        .steps = 12,
        .check_quorum = true,
        .pre_vote = true,
        .profile = .stress,
    });
    defer second.deinit();

    const first_json = try first.toJson(std.testing.allocator);
    defer std.testing.allocator.free(first_json);
    const second_json = try second.toJson(std.testing.allocator);
    defer std.testing.allocator.free(second_json);

    try std.testing.expectEqualStrings(first_json, second_json);
}
