// Copyright 2026 Antfly, Inc.
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
const backpressure_iface = @import("backpressure_iface.zig");

pub const Limits = struct {
    max_message_count: ?usize = null,
    max_message_bytes: ?usize = null,
    max_committed_entries: ?usize = null,
    max_committed_entry_bytes: ?usize = null,
    max_unstable_entries: ?usize = null,
    max_unstable_entry_bytes: ?usize = null,
    max_snapshot_bytes: ?usize = null,
};

pub const LimitBackpressure = struct {
    limits: Limits,
    calls: usize = 0,
    denials: usize = 0,

    pub fn init(limits: Limits) LimitBackpressure {
        return .{ .limits = limits };
    }

    pub fn policy(self: *LimitBackpressure) backpressure_iface.Backpressure {
        return .{
            .ptr = self,
            .vtable = &.{
                .allow_ready = allowReady,
            },
        };
    }

    fn allowReady(ptr: *anyopaque, pressure: backpressure_iface.ReadyPressure) bool {
        const self: *LimitBackpressure = @ptrCast(@alignCast(ptr));
        self.calls += 1;

        const allowed =
            within(self.limits.max_message_count, pressure.message_count) and
            within(self.limits.max_message_bytes, pressure.message_bytes) and
            within(self.limits.max_committed_entries, pressure.committed_entries) and
            within(self.limits.max_committed_entry_bytes, pressure.committed_entry_bytes) and
            within(self.limits.max_unstable_entries, pressure.unstable_entries) and
            within(self.limits.max_unstable_entry_bytes, pressure.unstable_entry_bytes) and
            within(self.limits.max_snapshot_bytes, pressure.snapshot_bytes);

        if (!allowed) self.denials += 1;
        return allowed;
    }

    fn within(limit: ?usize, actual: usize) bool {
        return actual <= (limit orelse std.math.maxInt(usize));
    }
};

test "limit backpressure denies oversized ready pressure" {
    var policy = LimitBackpressure.init(.{
        .max_message_bytes = 8,
        .max_snapshot_bytes = 4,
    });

    try std.testing.expect(policy.policy().allowReady(.{
        .group_id = 1,
        .message_count = 1,
        .message_bytes = 8,
        .committed_entries = 0,
        .committed_entry_bytes = 0,
        .unstable_entries = 0,
        .unstable_entry_bytes = 0,
        .has_snapshot = false,
        .snapshot_bytes = 0,
    }));
    try std.testing.expect(!policy.policy().allowReady(.{
        .group_id = 1,
        .message_count = 1,
        .message_bytes = 9,
        .committed_entries = 0,
        .committed_entry_bytes = 0,
        .unstable_entries = 0,
        .unstable_entry_bytes = 0,
        .has_snapshot = true,
        .snapshot_bytes = 8,
    }));
    try std.testing.expectEqual(@as(usize, 2), policy.calls);
    try std.testing.expectEqual(@as(usize, 1), policy.denials);
}
