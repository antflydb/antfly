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

const storage_source_options = @import("storage_source_options");
pub const raft_apply_store = @import("raft_apply_store.zig");
const raft_apply_client = @import("../../storage/metadata_raft_apply_client.zig");
pub const RaftApplyStore = if (storage_source_options.control_only) raft_apply_client.RaftApplyStore else raft_apply_store.RaftApplyStore;
pub const RaftApplyStoreConfig = if (storage_source_options.control_only) raft_apply_client.RaftApplyStoreConfig else raft_apply_store.RaftApplyStoreConfig;
pub const AppliedMetadataBatch = raft_apply_store.AppliedMetadataBatch;
pub const TransitionCommand = raft_apply_store.TransitionCommand;
pub const ExtensionLifecycleDelta = raft_apply_store.ExtensionLifecycleDelta;
pub const ExtensionLifecycleTablePrecondition = raft_apply_store.ExtensionLifecycleTablePrecondition;
pub const ExtensionMemberKey = raft_apply_store.ExtensionMemberKey;
pub const ExtensionDependencyKey = raft_apply_store.ExtensionDependencyKey;
pub const validateTransitionCommandDataGroupIds = raft_apply_store.validateTransitionCommandDataGroupIds;
pub const encodeTransitionCommand = raft_apply_store.encodeTransitionCommand;
pub const decodeTransitionCommand = raft_apply_store.decodeTransitionCommand;

test "metadata storage module compiles" {
    _ = RaftApplyStore;
    _ = RaftApplyStoreConfig;
    _ = AppliedMetadataBatch;
    _ = TransitionCommand;
    _ = ExtensionLifecycleDelta;
    _ = ExtensionLifecycleTablePrecondition;
    _ = ExtensionMemberKey;
    _ = ExtensionDependencyKey;
    _ = validateTransitionCommandDataGroupIds;
    _ = encodeTransitionCommand;
    _ = decodeTransitionCommand;
}
