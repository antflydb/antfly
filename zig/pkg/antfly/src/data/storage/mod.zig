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

pub const range_transition = @import("range_transition.zig");
pub const shard_state_store = @import("shard_state_store.zig");
pub const raft_apply_store = @import("raft_apply_store.zig");
pub const db_split_handoff = @import("db_split_handoff.zig");
pub const RaftApplyStore = raft_apply_store.RaftApplyStore;
pub const RaftApplyStoreConfig = raft_apply_store.RaftApplyStoreConfig;
pub const AppliedDataBatch = raft_apply_store.AppliedDataBatch;
pub const AppliedDataKV = shard_state_store.AppliedDataKV;
pub const AppliedDataRange = shard_state_store.AppliedDataRange;
pub const DataOperation = shard_state_store.DataOperation;
pub const RangeTransitionKind = range_transition.TransitionKind;
pub const RangeTransitionRole = range_transition.TransitionRole;
pub const RangeTransitionPhase = range_transition.TransitionPhase;
pub const SplitTransitionStatus = range_transition.SplitStatus;
pub const MergeTransitionStatus = range_transition.MergeStatus;
pub const MergeParticipantStatus = range_transition.MergeParticipantStatus;
pub const MergePairError = range_transition.MergePairError;
pub const SplitDestination = db_split_handoff.Destination;
pub const SplitDestinationConfig = db_split_handoff.DestinationConfig;
pub const MergeCoordinator = db_split_handoff.MergeCoordinator;
pub const MergeConfig = db_split_handoff.MergeConfig;
pub const MergeSyncStatus = db_split_handoff.MergeSyncStatus;
pub const SplitTransitionPhase = db_split_handoff.SplitTransitionPhase;
pub const SplitSyncCoordinator = db_split_handoff.SyncCoordinator;
pub const SplitSyncConfig = db_split_handoff.SyncConfig;
pub const SplitSyncStatus = db_split_handoff.SplitSyncStatus;
pub const observeSplitStatus = db_split_handoff.observeSplitStatus;

test "data storage module compiles" {
    _ = range_transition;
    _ = shard_state_store;
    _ = RaftApplyStore;
    _ = RaftApplyStoreConfig;
    _ = AppliedDataBatch;
    _ = AppliedDataKV;
    _ = AppliedDataRange;
    _ = DataOperation;
    _ = RangeTransitionKind;
    _ = RangeTransitionRole;
    _ = RangeTransitionPhase;
    _ = SplitTransitionStatus;
    _ = MergeTransitionStatus;
    _ = MergeParticipantStatus;
    _ = MergePairError;
    _ = SplitDestination;
    _ = SplitDestinationConfig;
    _ = MergeCoordinator;
    _ = MergeConfig;
    _ = MergeSyncStatus;
    _ = SplitTransitionPhase;
    _ = SplitSyncCoordinator;
    _ = SplitSyncConfig;
    _ = SplitSyncStatus;
    _ = observeSplitStatus;
}
