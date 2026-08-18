// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

const std = @import("std");

/// Metadata operations that lost their linearizable authority may be retried
/// against the current leader. Keep this classification shared so embedded,
/// public HTTP, and metadata HTTP paths expose the same availability contract.
pub fn isRetryableError(err: anyerror) bool {
    return switch (err) {
        error.NotLeader,
        error.ProposalDropped,
        error.LeaderTransferInProgress,
        error.MetadataLinearizableReadTimeout,
        error.ReconcileLeaseNotHeld,
        => true,
        else => false,
    };
}

/// Side-effecting clients may replay a mutation only when Raft rejected it
/// before assigning a log index. Keep this deliberately narrower than the
/// general authority retry set: read timeouts and reconcile-lease failures do
/// not prove that a preceding mutation was never admitted.
pub fn isMutationNotAdmittedError(err: anyerror) bool {
    return switch (err) {
        error.NotLeader,
        error.ProposalDropped,
        error.LeaderTransferInProgress,
        => true,
        else => false,
    };
}

test "metadata authority retry classification is fail closed" {
    try std.testing.expect(isRetryableError(error.NotLeader));
    try std.testing.expect(isRetryableError(error.ProposalDropped));
    try std.testing.expect(isRetryableError(error.LeaderTransferInProgress));
    try std.testing.expect(isRetryableError(error.MetadataLinearizableReadTimeout));
    try std.testing.expect(isRetryableError(error.ReconcileLeaseNotHeld));
    try std.testing.expect(!isRetryableError(error.InvalidArguments));
    try std.testing.expect(!isRetryableError(error.OutOfMemory));
}

test "metadata mutation non-admission classification excludes ambiguous authority failures" {
    try std.testing.expect(isMutationNotAdmittedError(error.NotLeader));
    try std.testing.expect(isMutationNotAdmittedError(error.ProposalDropped));
    try std.testing.expect(isMutationNotAdmittedError(error.LeaderTransferInProgress));
    try std.testing.expect(!isMutationNotAdmittedError(error.MetadataLinearizableReadTimeout));
    try std.testing.expect(!isMutationNotAdmittedError(error.ReconcileLeaseNotHeld));
    try std.testing.expect(!isMutationNotAdmittedError(error.OutOfMemory));
}
