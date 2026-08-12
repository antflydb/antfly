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

//! Data-plane domain types without the command runtime entry point.

pub const storage = @import("storage/mod.zig");
pub const raft_batch = @import("raft_batch.zig");

pub const RaftApplyStore = storage.RaftApplyStore;
pub const RaftApplyStoreConfig = storage.RaftApplyStoreConfig;
pub const AppliedDataBatch = storage.AppliedDataBatch;
pub const AppliedDataKV = storage.AppliedDataKV;
pub const AppliedDataRange = storage.AppliedDataRange;
pub const DataOperation = storage.DataOperation;
pub const RangeTransitionKind = storage.RangeTransitionKind;
pub const RangeTransitionRole = storage.RangeTransitionRole;
pub const RangeTransitionPhase = storage.RangeTransitionPhase;
pub const SplitTransitionStatus = storage.SplitTransitionStatus;
pub const MergeTransitionStatus = storage.MergeTransitionStatus;
pub const MergeParticipantStatus = storage.MergeParticipantStatus;
pub const MergePairError = storage.MergePairError;
pub const SplitDestination = storage.SplitDestination;
pub const SplitDestinationConfig = storage.SplitDestinationConfig;
pub const MergeCoordinator = storage.MergeCoordinator;
pub const MergeConfig = storage.MergeConfig;
pub const MergeSyncStatus = storage.MergeSyncStatus;
pub const SplitTransitionPhase = storage.SplitTransitionPhase;
pub const SplitSyncCoordinator = storage.SplitSyncCoordinator;
pub const SplitSyncConfig = storage.SplitSyncConfig;
pub const SplitSyncStatus = storage.SplitSyncStatus;
