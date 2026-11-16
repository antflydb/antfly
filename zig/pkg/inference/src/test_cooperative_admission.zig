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
const Lane = @import("runtime/scheduler/generation_admission.zig").Lane;

test "cooperative sustained admission turnover preserves exclusive ownership and queued deadline cleanup" {
    const Drain = struct {
        first: Lane.Lease,
        second: Lane.Lease,
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 2) self.first.release();
            if (self.checks == 3) self.second.release();
        }
    };
    const Deadline = struct {
        lane: *Lane,
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 2) {
                try std.testing.expectEqual(@as(usize, 1), self.lane.snapshot().waiting);
                return error.Timeout;
            }
        }
    };
    var lane = Lane{};
    var last_owner: u64 = 0;
    for (0..64) |_| {
        const first = try lane.acquire(.{}, false, .{ .active = 2, .queued = 4 });
        const second = try lane.acquire(.{}, false, .{ .active = 2, .queued = 4 });
        try std.testing.expect(first.owner_id > last_owner and second.owner_id > first.owner_id);
        var drain = Drain{ .first = first, .second = second };
        const exclusive = try lane.acquire(.{ .ptr = &drain, .check_fn = Drain.check }, true, .{ .active = 2, .queued = 4 });
        try std.testing.expectEqual(@as(usize, 3), drain.checks);
        try std.testing.expect(lane.exclusive and lane.snapshot().active == 1);
        var deadline = Deadline{ .lane = &lane };
        try std.testing.expectError(error.Timeout, lane.acquire(.{ .ptr = &deadline, .check_fn = Deadline.check }, false, .{ .active = 2, .queued = 4 }));
        try std.testing.expect(lane.head == null and lane.tail == null);
        try std.testing.expectEqual(@as(usize, 0), lane.snapshot().waiting);
        last_owner = exclusive.owner_id;
        exclusive.release();
        try std.testing.expectEqual(@as(usize, 0), lane.snapshot().active);
    }
    try std.testing.expectEqual(@as(u64, 192), lane.snapshot().started);
    try std.testing.expectEqual(@as(u64, 64), lane.snapshot().cancelled);
}

test "cooperative sustained unlimited admission preserves parallelism and exclusive fences" {
    const Limits = @import("runtime/scheduler/generation_admission.zig").Limits;
    const unlimited = Limits.forRequests(0, null);
    try std.testing.expect(unlimited.active == null);
    try std.testing.expectEqual(@as(usize, 64), unlimited.queued);
    var lane = Lane{};
    // A control budget makes accidental serialization fail instead of hanging.
    const Probe = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks > 2) return error.Timeout;
        }
    };
    var leases: [4]Lane.Lease = undefined;
    for (&leases) |*lease| {
        var probe = Probe{};
        lease.* = try lane.acquire(.{ .ptr = &probe, .check_fn = Probe.check }, false, unlimited);
    }
    try std.testing.expectEqual(@as(usize, 4), lane.snapshot().active);
    const Drain = struct {
        leases: []Lane.Lease,
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 3) for (self.leases) |lease| lease.release();
            if (self.checks > 3) return error.Timeout;
        }
    };
    var drain = Drain{ .leases = &leases };
    const exclusive = try lane.acquire(.{ .ptr = &drain, .check_fn = Drain.check }, true, unlimited);
    try std.testing.expectEqual(@as(usize, 3), drain.checks);
    try std.testing.expect(lane.exclusive and lane.snapshot().active == 1);
    var probe = Probe{};
    try std.testing.expectError(error.Timeout, lane.acquire(.{ .ptr = &probe, .check_fn = Probe.check }, false, unlimited));
    try std.testing.expectEqual(@as(usize, 0), lane.snapshot().waiting);
    exclusive.release();
    const next = try lane.acquire(.{}, false, unlimited);
    next.release();
}

test "cooperative sustained explicit CUDA cap survives unlimited node admission" {
    const Limits = @import("runtime/scheduler/generation_admission.zig").Limits;
    const limits = Limits.forRequests(0, 2);
    try std.testing.expectEqual(@as(?usize, 2), limits.active);
    try std.testing.expectEqual(@as(usize, 64), limits.queued);
    try std.testing.expectEqual(@as(?usize, 32), Limits.forRequests(32, null).active);
    try std.testing.expectEqual(@as(usize, 32), Limits.forRequests(32, null).queued);
    try std.testing.expectEqual(@as(usize, 64), Limits.forRequests(128, null).queued);
    var lane = Lane{};
    const first = try lane.acquire(.{}, false, limits);
    defer first.release();
    const second = try lane.acquire(.{}, false, limits);
    defer second.release();
    const Probe = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks > 2) return error.Timeout;
        }
    };
    var probe = Probe{};
    try std.testing.expectError(error.Timeout, lane.acquire(.{ .ptr = &probe, .check_fn = Probe.check }, false, limits));
    try std.testing.expectEqual(@as(usize, 2), lane.snapshot().active);
    try std.testing.expectEqual(@as(usize, 0), lane.snapshot().waiting);
}

test "cooperative sustained unlimited admission retains bounded waiting capacity" {
    const Limits = @import("runtime/scheduler/generation_admission.zig").Limits;
    const State = struct {
        lane: *Lane,
        depth: usize = 0,
        checks: [65]usize = @splat(0),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks[self.depth] += 1;
            if (self.checks[self.depth] != 2) return;
            try std.testing.expectEqual(self.depth + 1, self.lane.snapshot().waiting);
            self.depth += 1;
            defer self.depth -= 1;
            try std.testing.expectError(
                if (self.depth == 64) error.GenerationQueueFull else error.Cancelled,
                self.lane.acquire(.{ .ptr = self, .check_fn = check }, false, Limits.forRequests(0, null)),
            );
            return error.Cancelled;
        }
    };
    var lane = Lane{};
    const owner = try lane.acquire(.{}, true, Limits.forRequests(0, null));
    defer owner.release();
    var state = State{ .lane = &lane };
    try std.testing.expectError(error.Cancelled, lane.acquire(.{ .ptr = &state, .check_fn = State.check }, false, Limits.forRequests(0, null)));
    const stats = lane.snapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.waiting);
    try std.testing.expectEqual(@as(u64, 64), stats.cancelled);
    try std.testing.expectEqual(@as(u64, 1), stats.rejected);
}
