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

const transition_state = @import("transition_state.zig");

// Named payloads keep the checked callback type fingerprints stable across
// independently compiled runtime archives; anonymous union payload names carry
// compilation-local suffixes even when their physical layouts are identical.
pub const PrepareSplitSource = struct {
    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    split_key: []const u8,
    source_range_end: ?[]const u8 = null,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const StartSplitSource = struct {
    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const BootstrapSplitDestination = struct {
    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const CatchUpSplitDestination = struct {
    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const FinalizeSplitSource = struct {
    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const RollbackSplit = struct {
    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const AcceptMergeReceiver = struct {
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    allow_doc_identity_reassignment: bool = false,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const CatchUpMergeReceiver = struct {
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    allow_doc_identity_reassignment: bool = false,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const FinalizeMerge = struct {
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    allow_doc_identity_reassignment: bool = false,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const RollbackMerge = struct {
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    allow_doc_identity_reassignment: bool = false,
    table_contract: transition_state.TransitionTableContract = .{},
};

pub const TransitionAction = union(enum) {
    none,
    prepare_split_source: PrepareSplitSource,
    start_split_source: StartSplitSource,
    bootstrap_split_destination: BootstrapSplitDestination,
    catch_up_split_destination: CatchUpSplitDestination,
    finalize_split_source: FinalizeSplitSource,
    rollback_split: RollbackSplit,
    accept_merge_receiver: AcceptMergeReceiver,
    catch_up_merge_receiver: CatchUpMergeReceiver,
    finalize_merge: FinalizeMerge,
    rollback_merge: RollbackMerge,
};

pub const TransitionDecision = struct {
    next_phase: transition_state.TransitionPhase,
    action: TransitionAction,
};

test "transition actions module compiles" {
    _ = TransitionAction;
    _ = TransitionDecision;
}
