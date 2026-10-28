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

const std = @import("std");

/// Selection bookkeeping lives under the owning registry mutex. Attempts count
/// even when storage reports busy or fails: one unproductive owner cannot keep
/// every other eligible owner from receiving a turn. This is a selection bound,
/// not a bound on the physical duration of a storage step.
pub const Policy = struct {
    epoch: u64 = 0,
    fair_turn: bool = false,

    pub const Candidate = struct {
        last_served: u64,
        score: u64,
        due: bool = false,
        /// Preserve the local DB's shared/primary fallback on priority turns.
        priority: u8 = 0,

        pub fn eligible(self: @This()) bool {
            return self.due or self.score != 0;
        }
    };

    pub fn prefers(self: Policy, candidate: Candidate, current: ?Candidate) bool {
        if (!candidate.eligible()) return false;
        const previous = current orelse return true;
        const age = self.epoch -% candidate.last_served;
        const previous_age = self.epoch -% previous.last_served;
        if (self.fair_turn and age != previous_age) return age > previous_age;
        if (candidate.due != previous.due) return candidate.due;
        if (candidate.priority != previous.priority) return candidate.priority > previous.priority;
        if (candidate.score != previous.score) return candidate.score > previous.score;
        return age > previous_age;
    }

    /// New and transferred registry entries join the tail at the current epoch.
    /// The stamp and policy must be updated together under the registry lock.
    pub fn selected(self: *Policy, last_served: *u64) void {
        self.epoch +%= 1;
        last_served.* = self.epoch;
        self.fair_turn = !self.fair_turn;
    }
};

/// One bounded registry window per round. Wide registries alternate pressure
/// and fair policy by whole sweep; alternating by window would permanently
/// assign the same policy to each window when the window count is even.
pub const Scan = struct {
    pub const inspection_limit = 64;
    cursor: usize = 0,
    fair_sweep: bool = false,
    pub const Window = struct {
        start: usize,
        limit: usize,
        more: bool,
        policy: Policy,
    };
    /// Caller serializes selection, including advancing past failed/no-work
    /// windows. Registry churn is re-observed at the next bounded round.
    pub fn next(self: *Scan, count: usize, policy: Policy) Window {
        if (count <= inspection_limit) {
            self.cursor = 0;
            return .{ .start = 0, .limit = count, .more = false, .policy = policy };
        }
        if (self.cursor >= count) self.cursor = 0;
        const start = self.cursor;
        const limit = start + @min(inspection_limit, count - start);
        var selected_policy = policy;
        selected_policy.fair_turn = self.fair_sweep;
        self.cursor = if (limit == count) 0 else limit;
        if (limit == count) self.fair_sweep = !self.fair_sweep;
        return .{ .start = start, .limit = limit, .more = limit < count, .policy = selected_policy };
    }
};

fn choose(policy: Policy, candidates: []const Policy.Candidate) ?usize {
    var selected: ?usize = null;
    for (candidates, 0..) |candidate, i| {
        if (policy.prefers(candidate, if (selected) |j| candidates[j] else null)) selected = i;
    }
    return selected;
}

test "maintenance selection bounds service gaps under permanent hot and due debt" {
    // Exercise priority inversion, first-entry due bias, zero-score due work,
    // and the primary-only fallback together. Every continuously eligible
    // owner must get a turn within twice the population's selections.
    for ([_]u64{ 0, std.math.maxInt(u64) - 5 }) |start| {
        var policy = Policy{ .epoch = start };
        var candidates = [_]Policy.Candidate{
            .{ .last_served = start, .score = std.math.maxInt(u64), .due = true, .priority = 1 },
            .{ .last_served = start, .score = 1 },
            .{ .last_served = start, .score = 0, .due = true },
            .{ .last_served = start, .score = 1, .priority = 1 },
            .{ .last_served = start, .score = 0 },
        };
        var gaps: [4]usize = @splat(0);
        var hot_turns: usize = 0;
        for (0..256) |turn| {
            const i = choose(policy, &candidates).?;
            try std.testing.expect(i < gaps.len);
            if (turn % 2 == 0) try std.testing.expectEqual(@as(usize, 0), i);
            if (i == 0) hot_turns += 1;
            for (&gaps, 0..) |*gap, j| {
                gap.* = if (j == i) 0 else gap.* + 1;
                try std.testing.expect(gap.* < 2 * gaps.len);
            }
            policy.selected(&candidates[i].last_served);
        }
        try std.testing.expect(hot_turns >= 128);
    }
}

test "maintenance selection new owners cannot jump an older eligible waiter" {
    var policy = Policy{};
    var candidates = [_]Policy.Candidate{
        .{ .last_served = 0, .score = 1000, .due = true },
        .{ .last_served = 0, .score = 1 },
        .{ .last_served = 0, .score = 100 },
    };
    var service_gap: usize = 0;
    for (0..128) |_| {
        // Replacing this owner every round cannot repeatedly jump the waiter.
        candidates[2].last_served = policy.epoch;
        const i = choose(policy, &candidates).?;
        service_gap = if (i == 1) 0 else service_gap + 1;
        try std.testing.expect(service_gap < 2 * candidates.len);
        policy.selected(&candidates[i].last_served);
    }
    candidates[1].score = 0;
    for (0..32) |_| {
        const i = choose(policy, &candidates).?;
        try std.testing.expect(i != 1);
        policy.selected(&candidates[i].last_served);
    }
    try std.testing.expect(choose(policy, &.{}) == null);
    try std.testing.expect(choose(policy, &.{.{ .last_served = 0, .score = 0 }}) == null);
}
