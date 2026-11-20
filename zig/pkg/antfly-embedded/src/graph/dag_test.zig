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

//! Composition tests for the validation kernel and authority value protocol.
//! These do not substitute for production Raft/transaction integration tests.
const std = @import("std");
const validation = @import("dag_validation.zig");
const authority = @import("dag_authority.zig");
const A = std.mem.Allocator;
const Clock = struct {
    fn now(_: *anyopaque) u64 {
        return 1;
    }
    fn budget(self: *@This()) validation.Budget {
        return .{ .clock = .{ .context = self, .now = now } };
    }
};
const Reader = struct {
    edge: ?validation.Change = null,
    pub fn next(self: @This(), _: A, source: []const u8, after: ?[]const u8, _: *validation.Budget) !?validation.Row {
        if (after != null) return null;
        const edge = self.edge orelse return null;
        if (!std.mem.eql(u8, source, edge.source)) return null;
        return .{ .key = edge.target, .target = edge.target };
    }
};

test "DAG protocol opposing candidates require reservation and revalidation at the accepted revision" {
    const changes = [_]validation.Change{
        .{ .source = "left", .target = "right", .present = true },
        .{ .source = "right", .target = "left", .present = true },
    };
    const attempts = [_]authority.Attempt{
        .{ .id = @splat(1), .transaction = @splat(1), .candidate = @splat(1) },
        .{ .id = @splat(2), .transaction = @splat(2), .candidate = @splat(2) },
    };
    // Both isolated traversals pass. They cannot authorize independent commits.
    var clock: Clock = .{};
    var witness: validation.Witness = .{};
    for (changes) |change| {
        var budget = clock.budget();
        try validation.validate(std.testing.allocator, Reader{}, &.{change}, &budget, &witness);
    }
    for (0..2) |winner| {
        const loser = 1 - winner;
        inline for (.{ false, true }) |takeover| {
            var state: authority.State = .{ .domain = @splat(1), .owner_epoch = 1, .phase = .active };
            var token = try state.begin(1, 0, attempts[winner]);
            try std.testing.expectError(error.GraphConstraintBusy, state.begin(1, 0, attempts[loser]));
            var budget = clock.budget();
            try validation.validate(std.testing.allocator, Reader{}, &.{changes[winner]}, &budget, &witness);
            try state.validated(token);
            // A restart/owner transfer after validation must keep the domain
            // fenced until the original durable transaction decision resolves.
            state = try authority.State.decode(&try state.encode());
            if (takeover) {
                try state.takeover(1, 2);
                try std.testing.expectError(error.GraphConstraintOwnerChanged, state.resolve(token, .accepted));
                token = (try state.recoveryToken(2)).?;
            }
            _ = try state.resolve(token, .accepted);
            const epoch = state.owner_epoch;
            try std.testing.expectError(error.GraphConstraintRevisionChanged, state.begin(epoch, 0, attempts[loser]));
            const next = try state.begin(epoch, 1, attempts[loser]);
            budget = clock.budget();
            try std.testing.expectError(error.GraphCycle, validation.validate(std.testing.allocator, Reader{ .edge = changes[winner] }, &.{changes[loser]}, &budget, &witness));
            const rejected = try state.resolve(next, .semantic_rejection);
            try std.testing.expectEqual(authority.Outcome.semantic_rejection, rejected.outcome);
            try std.testing.expectEqual(@as(u64, 1), state.revision);
            try std.testing.expect(state.pending == null);
        }
    }
}
