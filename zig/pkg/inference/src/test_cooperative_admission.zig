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
        const first = try lane.acquire(.{}, false, 2, 4);
        const second = try lane.acquire(.{}, false, 2, 4);
        try std.testing.expect(first.owner_id > last_owner and second.owner_id > first.owner_id);
        var drain = Drain{ .first = first, .second = second };
        const exclusive = try lane.acquire(.{ .ptr = &drain, .check_fn = Drain.check }, true, 2, 4);
        try std.testing.expectEqual(@as(usize, 3), drain.checks);
        try std.testing.expect(lane.exclusive and lane.snapshot().active == 1);
        var deadline = Deadline{ .lane = &lane };
        try std.testing.expectError(error.Timeout, lane.acquire(.{ .ptr = &deadline, .check_fn = Deadline.check }, false, 2, 4));
        try std.testing.expect(lane.head == null and lane.tail == null);
        try std.testing.expectEqual(@as(usize, 0), lane.snapshot().waiting);
        last_owner = exclusive.owner_id;
        exclusive.release();
        try std.testing.expectEqual(@as(usize, 0), lane.snapshot().active);
    }
    try std.testing.expectEqual(@as(u64, 192), lane.snapshot().started);
    try std.testing.expectEqual(@as(u64, 64), lane.snapshot().cancelled);
}
