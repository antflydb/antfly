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

//! Bounded physical work coalescing. Notifications never renew the first
//! debt's deadline; pressure and full batches bypass the quiet-time delay.
const std = @import("std");

pub fn Batch(comptime delay_ns: u64, comptime minimum_items: u64) type {
    return struct {
        pub const maximum_delay_ns = delay_ns;
        first_pending_ns: ?i96 = null,

        pub fn due(self: *@This(), now_ns: i96, items: u64, work_budget: usize, pressure: bool) bool {
            if (items == 0 and !pressure) {
                self.first_pending_ns = null;
                return false;
            }
            if (self.first_pending_ns == null) self.first_pending_ns = now_ns;
            return pressure or items >= @max(minimum_items, work_budget) or now_ns >= self.deadline().?;
        }
        pub fn deadline(self: @This()) ?i96 {
            return if (self.first_pending_ns) |first| first +| maximum_delay_ns else null;
        }
        pub fn serviced(self: *@This()) void {
            self.first_pending_ns = null;
        }
    };
}

test "physical maintenance batching retains debt age across notifications and refresh" {
    const Policy = Batch(100 * std.time.ns_per_ms, 64);
    var policy: Policy = .{};
    try std.testing.expect(!policy.due(0, 1, 64, false));
    const deadline = policy.deadline().?;
    for (1..100) |ms| try std.testing.expect(!policy.due(@as(i96, @intCast(ms)) * std.time.ns_per_ms, 1 + ms / 2, 64, false));
    var inherited = policy;
    try std.testing.expect(inherited.due(deadline, 1, 64, false));
    inherited.serviced();
    try std.testing.expect(inherited.due(deadline + 1, 64, 64, false));
    inherited.serviced();
    try std.testing.expect(inherited.due(deadline + 2, 1, 64, true));
    try std.testing.expect(!inherited.due(deadline + 3, 0, 64, false));
    try std.testing.expect(inherited.deadline() == null);
}
