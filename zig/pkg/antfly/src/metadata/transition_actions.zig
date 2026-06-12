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

pub const TransitionAction = union(enum) {
    none,
    prepare_split_source: struct {
        transition_id: u64,
        source_group_id: u64,
        destination_group_id: u64,
        split_key: []const u8,
        source_range_end: ?[]const u8 = null,
    },
    start_split_source: struct {
        transition_id: u64,
        source_group_id: u64,
        destination_group_id: u64,
    },
    bootstrap_split_destination: struct {
        transition_id: u64,
        source_group_id: u64,
        destination_group_id: u64,
    },
    catch_up_split_destination: struct {
        transition_id: u64,
        source_group_id: u64,
        destination_group_id: u64,
    },
    finalize_split_source: struct {
        transition_id: u64,
        source_group_id: u64,
        destination_group_id: u64,
    },
    rollback_split: struct {
        transition_id: u64,
        source_group_id: u64,
        destination_group_id: u64,
    },
    accept_merge_receiver: struct {
        transition_id: u64,
        donor_group_id: u64,
        receiver_group_id: u64,
        allow_doc_identity_reassignment: bool = false,
    },
    catch_up_merge_receiver: struct {
        transition_id: u64,
        donor_group_id: u64,
        receiver_group_id: u64,
        allow_doc_identity_reassignment: bool = false,
    },
    finalize_merge: struct {
        transition_id: u64,
        donor_group_id: u64,
        receiver_group_id: u64,
        allow_doc_identity_reassignment: bool = false,
    },
    rollback_merge: struct {
        transition_id: u64,
        donor_group_id: u64,
        receiver_group_id: u64,
    },
};

pub const TransitionDecision = struct {
    next_phase: transition_state.TransitionPhase,
    action: TransitionAction,
};

test "transition actions module compiles" {
    _ = TransitionAction;
    _ = TransitionDecision;
}
