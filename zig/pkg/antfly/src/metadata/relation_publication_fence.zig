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

//! Replica-local dependency evidence for bounded catalog proof preparation.
//! This is not publication authority or a durable source-adoption mechanism.
//! The owner serializes access and records committed Raft watermarks. Other
//! mutation ports invalidate the domain; unmatched cuts use exact positions.
const std = @import("std");

pub const Cut = struct {
    generation: u64,
    revision: u64,
    tracked: bool,
};

pub const State = struct {
    generation: u64 = 1,
    revision: u64 = 1,
    applied_index: ?u64 = null,
    input_index: u64 = 0,
    enabled: bool = true,

    /// Call after a successful Raft commit, while its apply lane is held.
    /// Neutrality is a closed owner-side whitelist, never a request property.
    /// An unknown writer, restart, or observation gap breaks the prior domain
    /// even when the newly observed command itself is unrelated to the proof.
    pub fn observe(self: *State, before: u64, after: u64, neutral: bool) void {
        if (!self.enabled) return;
        if (after <= before) {
            self.invalidate();
            return;
        }
        if (self.applied_index == null or self.applied_index.? != before or !neutral) {
            self.advanceRevision();
            self.input_index = after;
        }
        self.applied_index = if (self.enabled) after else null;
    }

    /// Bracket foreign/non-Raft mutation and namespace installation with this
    /// invalidation, including the completion/abort boundary. No proof from a
    /// previous namespace or an in-flight mutation may be silently adopted.
    pub fn invalidate(self: *State) void {
        self.applied_index = null;
        if (!self.enabled) return;
        self.generation = std.math.add(u64, self.generation, 1) catch {
            self.enabled = false;
            return;
        };
        self.advanceRevision();
    }

    fn advanceRevision(self: *State) void {
        self.revision = std.math.add(u64, self.revision, 1) catch {
            // Exhaustion disables reuse until owner restart. Never wrap a cut
            // or fail an operation after its storage commit has succeeded.
            self.enabled = false;
            return;
        };
    }

    /// Capture after opening the immutable store read. A concurrent commit can
    /// make the read older/newer than this observation, but cannot manufacture
    /// a tracked match: only the completely observed neutral interval after
    /// the last input mutation shares a domain; other reads use strict cuts.
    pub fn capture(self: State, read_index: u64) Cut {
        const tracked = self.enabled and self.applied_index != null and
            read_index >= self.input_index and read_index <= self.applied_index.?;
        return .{
            .generation = self.generation,
            .revision = if (tracked) self.revision else read_index,
            .tracked = tracked,
        };
    }

    /// Source/job/root and capability validation remain the caller's duty.
    /// This only certifies that a completed scan can be rebound to a newer
    /// exact applied position without skipping any unknown input mutation.
    pub fn canAdopt(self: State, prior: Cut, prior_index: u64, current_index: u64) bool {
        if (!self.enabled or current_index < prior_index) return false;
        const current = self.capture(current_index);
        if (!std.meta.eql(prior, current)) return false;
        return current.tracked or current_index == prior_index;
    }
};

test "relation publication fence retains only completely observed neutral progress" {
    var state: State = .{};
    const initial = state.capture(0);
    try std.testing.expect(!initial.tracked);
    try std.testing.expect(state.canAdopt(initial, 0, 0));
    try std.testing.expect(!state.canAdopt(initial, 0, 1));
    state.observe(0, 1, false);
    const cut = state.capture(1);
    try std.testing.expect(cut.tracked);
    for (1..100) |index| {
        state.observe(index, index + 1, true);
        try std.testing.expectEqualDeep(cut, state.capture(index + 1));
        try std.testing.expectEqualDeep(cut, state.capture(1));
        try std.testing.expect(state.canAdopt(cut, 1, index + 1));
    }
    state.observe(100, 101, false);
    try std.testing.expect(!state.canAdopt(cut, 1, 101));
}

test "relation publication fence rejects observation gaps stale reads and namespace reuse" {
    var state: State = .{};
    state.observe(0, 1, true);
    const first = state.capture(1);
    try std.testing.expect(!state.capture(0).tracked);
    try std.testing.expect(!state.capture(2).tracked);
    state.observe(3, 4, true); // another writer advanced the durable watermark
    try std.testing.expect(!state.canAdopt(first, 1, 4));
    const second = state.capture(4);
    state.invalidate();
    try std.testing.expect(!state.canAdopt(second, 4, 4));
    state.observe(3, 4, true);
    try std.testing.expect(!state.canAdopt(second, 4, 4));
    const third = state.capture(4);
    state.observe(4, 4, true);
    try std.testing.expect(!state.canAdopt(third, 4, 4));
    try std.testing.expect(!state.canAdopt(state.capture(4), 4, 3));
}

test "relation publication fence exhaustion fails closed without wrapping" {
    for ([_]State{
        .{ .generation = std.math.maxInt(u64) },
        .{ .revision = std.math.maxInt(u64) },
    }) |initial| {
        var state = initial;
        const cut = state.capture(0);
        state.invalidate();
        try std.testing.expect(!state.enabled);
        try std.testing.expect(!state.capture(0).tracked);
        try std.testing.expect(!state.canAdopt(cut, 0, 0));
        state.observe(0, 1, true);
        try std.testing.expect(!state.canAdopt(state.capture(1), 1, 1));
    }
}
