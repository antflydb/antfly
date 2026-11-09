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

//! One non-recycling namespace for immutable source/cache values and their
//! transient descriptors. Targets must not create competing model-ID counters.
const std = @import("std");
pub const Generator = struct {
    next_value: std.atomic.Value(u64) = .init(1),
    pub fn tryNext(self: *Generator) error{ResourceLimitExceeded}!u64 {
        var value = self.next_value.load(.monotonic);
        while (true) {
            if (value == 0 or value == std.math.maxInt(u64)) return error.ResourceLimitExceeded;
            if (self.next_value.cmpxchgWeak(value, value + 1, .monotonic, .monotonic)) |observed| value = observed else return value;
        }
    }
};
var generator = Generator{};
pub fn next() u64 {
    return generator.tryNext() catch @panic("execution source identity exhausted");
}
test "execution source identities never recycle after exhaustion" {
    var local = Generator{ .next_value = .init(std.math.maxInt(u64) - 1) };
    try std.testing.expectEqual(std.math.maxInt(u64) - 1, try local.tryNext());
    for (0..4) |_| try std.testing.expectError(error.ResourceLimitExceeded, local.tryNext());
    try std.testing.expectEqual(std.math.maxInt(u64), local.next_value.load(.acquire));
}
