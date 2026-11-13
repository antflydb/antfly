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

/// Expected immutable conflicts and absent optional objects cannot consume the
/// diagnostic allowance reserved for other responses. Each class is bounded.
pub const Budget = struct {
    expected: std.atomic.Value(usize) = .init(0),
    errors: std.atomic.Value(usize) = .init(0),
    pub fn admit(self: *@This(), status: u16) bool {
        const counter = if (status == 404 or status == 412) &self.expected else &self.errors;
        return counter.fetchAdd(1, .monotonic) < 64;
    }
    pub fn reset(self: *@This()) void {
        self.expected.store(0, .monotonic);
        self.errors.store(0, .monotonic);
    }
};

test "external lake execution generation publication diagnostics reserve terminal status capacity" {
    var budget: Budget = .{};
    for (0..1024) |i| _ = budget.admit(if (i % 2 == 0) 412 else 404);
    try std.testing.expect(!budget.admit(412));
    try std.testing.expect(budget.admit(503));
    try std.testing.expect(budget.admit(429));
    for (0..62) |_| try std.testing.expect(budget.admit(403));
    try std.testing.expect(!budget.admit(503));
    budget.reset();
    try std.testing.expect(budget.admit(412));
    try std.testing.expect(budget.admit(503));
}
