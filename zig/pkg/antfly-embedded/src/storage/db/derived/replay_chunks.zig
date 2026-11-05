// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! WS5: shared bookkeeping for owner-queued journal retirement.
//!
//! Foreground/ack callers never physically delete replay-journal entries.
//! They call `RetirementQueue.request` to raise a watermark. Only the active
//! owner (the Lite path-lease holder or the Raft leader) turns that watermark
//! into bytes: one bounded, single-transaction chunk per maintenance turn,
//! produced by `DocStore.serviceRetirement` / the lite `Store.retireReplaySlice`
//! using the primitives below.
const std = @import("std");

/// One retirement transaction's budget, shared across every lane. Keeping
/// the bound combined (not per-lane) is what turns "one commit per lane"
/// into "one commit per turn".
pub const RetirementBudget = struct {
    max_keys: usize = default_max_keys,
    max_bytes: usize = default_max_bytes,

    pub const default_max_keys: usize = 4096;
    pub const default_max_bytes: usize = 1 << 20; // 1 MiB of journal payload.
};

/// Outcome of one bounded retirement transaction.
pub const RetirementStats = struct {
    keys_deleted: usize = 0,
    bytes_deleted: usize = 0,
    /// Highest sequence this chunk retired through, across every lane it
    /// touched. Zero if nothing was eligible to retire.
    retired_through: u64 = 0,
    /// True once every lane has been fully drained up to the requested
    /// target (so another call with the same target is a no-op). False
    /// means the budget was exhausted first and the owner should call again
    /// on its next turn.
    done: bool = true,
    /// True if the transaction found nothing at all to do (no commit was
    /// issued). Distinguishing this from `done` lets a caller skip the loop
    /// optimistically rather than paying one empty transaction per lane.
    no_op: bool = true,
};

/// A retirement target can never outrun the slowest registered consumer: a
/// caller must request retirement only through its own watermark, and the
/// owner must further clamp that to whatever every consumer has actually
/// applied. Keeping the clamp as one pure function makes "retired sequence
/// stays <= min consumer progress" (design doc S6 test 6) checkable in
/// isolation from storage.
pub fn clampRetireTarget(requested_up_to: u64, min_consumer_progress: u64) u64 {
    return @min(requested_up_to, min_consumer_progress);
}

/// Monotonic watermark raised by foreground/ack callers and drained by the
/// owner-only retirement service. `request` only ever moves the watermark
/// forward: a stale or reordered caller can never push it backwards and
/// resurrect journal entries a faster caller already allowed to retire.
pub const RetirementQueue = struct {
    requested_through: std.atomic.Value(u64) = .init(0),

    pub fn request(self: *RetirementQueue, min_progress: u64) void {
        var current = self.requested_through.load(.monotonic);
        while (min_progress > current) {
            if (self.requested_through.cmpxchgWeak(current, min_progress, .monotonic, .monotonic)) |actual| {
                current = actual;
                continue;
            }
            return;
        }
    }

    pub fn requested(self: *const RetirementQueue) u64 {
        return self.requested_through.load(.monotonic);
    }
};

test "RetirementQueue.request only advances and never regresses" {
    var queue = RetirementQueue{};
    try std.testing.expectEqual(@as(u64, 0), queue.requested());
    queue.request(5);
    try std.testing.expectEqual(@as(u64, 5), queue.requested());
    queue.request(3);
    try std.testing.expectEqual(@as(u64, 5), queue.requested());
    queue.request(9);
    try std.testing.expectEqual(@as(u64, 9), queue.requested());
}

test "clampRetireTarget never exceeds the slowest consumer" {
    try std.testing.expectEqual(@as(u64, 4), clampRetireTarget(10, 4));
    try std.testing.expectEqual(@as(u64, 4), clampRetireTarget(4, 10));
    try std.testing.expectEqual(@as(u64, 0), clampRetireTarget(10, 0));
}
