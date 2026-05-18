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

pub const MergePairObservation = struct {
    donor_node_id: ?u64 = null,
    receiver_node_id: ?u64 = null,
    donor: data.MergeTransitionStatus,
    receiver: data.MergeTransitionStatus,
};

pub const MergeTransitionCheckError = data.MergePairError;
pub const TransitionEnrichmentCheckError = MergeTransitionCheckError || error{
    DestinationOwnsTransitionRangeBeforeCutover,
    ReceiverOwnsMergedRangeBeforeCutover,
    DonorOwnsMergedRangeAfterFinalize,
};

pub const SplitEnrichmentObservation = struct {
    source_node_id: ?u64 = null,
    destination_node_id: ?u64 = null,
    status: data.SplitTransitionStatus,
    destination_owns_transition_range: bool,
};

pub const MergeEnrichmentObservation = struct {
    donor_node_id: ?u64 = null,
    receiver_node_id: ?u64 = null,
    donor: data.MergeTransitionStatus,
    receiver: data.MergeTransitionStatus,
    donor_owns_merged_range: bool,
    receiver_owns_merged_range: bool,
};

pub fn validateMirroredMergePair(
    donor: data.MergeTransitionStatus,
    receiver: data.MergeTransitionStatus,
) MergeTransitionCheckError!void {
    try data.storage.range_transition.validateMirroredMergePair(
        data.storage.range_transition.donorParticipant(donor),
        data.storage.range_transition.receiverParticipant(receiver),
    );
}

pub fn validateObservedMergePair(observation: MergePairObservation) MergeTransitionCheckError!void {
    _ = observation.donor_node_id;
    _ = observation.receiver_node_id;
    try validateMirroredMergePair(observation.donor, observation.receiver);
}

pub fn validateSplitEnrichment(
    status: data.SplitTransitionStatus,
    destination_owns_transition_range: bool,
) TransitionEnrichmentCheckError!void {
    if (destination_owns_transition_range and !status.destination_ready_for_reads) {
        return error.DestinationOwnsTransitionRangeBeforeCutover;
    }
}

pub fn validateObservedSplitEnrichment(observation: SplitEnrichmentObservation) TransitionEnrichmentCheckError!void {
    _ = observation.source_node_id;
    _ = observation.destination_node_id;
    try validateSplitEnrichment(observation.status, observation.destination_owns_transition_range);
}

pub fn validateMergeEnrichment(
    donor: data.MergeTransitionStatus,
    receiver: data.MergeTransitionStatus,
    donor_owns_merged_range: bool,
    receiver_owns_merged_range: bool,
) TransitionEnrichmentCheckError!void {
    try validateMirroredMergePair(donor, receiver);
    if (receiver_owns_merged_range and !receiver.receiver_ready_for_reads) {
        return error.ReceiverOwnsMergedRangeBeforeCutover;
    }
    if (donor_owns_merged_range and donor.phase == .finalized) {
        return error.DonorOwnsMergedRangeAfterFinalize;
    }
}

pub fn validateObservedMergeEnrichment(observation: MergeEnrichmentObservation) TransitionEnrichmentCheckError!void {
    _ = observation.donor_node_id;
    _ = observation.receiver_node_id;
    try validateMergeEnrichment(
        observation.donor,
        observation.receiver,
        observation.donor_owns_merged_range,
        observation.receiver_owns_merged_range,
    );
}

test "transition checker accepts mirrored merge statuses" {
    const status = data.storage.range_transition.deriveMergeStatus(
        10,
        20,
        true,
        true,
        5,
        5,
        false,
        false,
        false,
    );
    try validateMirroredMergePair(status, status);
}

test "transition checker rejects asymmetric merge statuses" {
    const donor = data.storage.range_transition.deriveMergeStatus(
        10,
        20,
        true,
        true,
        5,
        5,
        false,
        false,
        false,
    );
    const receiver = data.storage.range_transition.deriveMergeStatus(
        10,
        20,
        true,
        true,
        5,
        3,
        false,
        false,
        false,
    );
    try std.testing.expectError(error.MismatchedPhase, validateMirroredMergePair(donor, receiver));
}

test "transition checker rejects split destination enrichment before cutover" {
    const replay = data.storage.range_transition.deriveSplitStatus(.splitting, true, 4, 3);
    try std.testing.expectError(
        error.DestinationOwnsTransitionRangeBeforeCutover,
        validateSplitEnrichment(replay, true),
    );

    const cutover = data.storage.range_transition.deriveSplitStatus(.splitting, true, 4, 4);
    try validateSplitEnrichment(cutover, true);
}

test "transition checker rejects merge receiver enrichment before cutover" {
    const donor = data.storage.range_transition.deriveMergeStatus(
        10,
        20,
        true,
        true,
        5,
        4,
        false,
        false,
        false,
    );
    const receiver = data.storage.range_transition.deriveMergeStatus(
        10,
        20,
        true,
        true,
        5,
        4,
        false,
        false,
        false,
    );
    try std.testing.expectError(
        error.ReceiverOwnsMergedRangeBeforeCutover,
        validateMergeEnrichment(donor, receiver, false, true),
    );
}

test "transition checker rejects merge donor enrichment after finalize" {
    const donor = data.storage.range_transition.deriveMergeStatus(
        10,
        20,
        true,
        true,
        5,
        5,
        false,
        true,
        false,
    );
    const receiver = data.storage.range_transition.deriveMergeStatus(
        10,
        20,
        true,
        true,
        5,
        5,
        false,
        true,
        false,
    );
    try std.testing.expectError(
        error.DonorOwnsMergedRangeAfterFinalize,
        validateMergeEnrichment(donor, receiver, true, true),
    );
    try validateMergeEnrichment(donor, receiver, false, true);
}
