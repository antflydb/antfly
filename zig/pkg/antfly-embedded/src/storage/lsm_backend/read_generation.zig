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

//! Process-local immutable run identity. The issuer is a static counter address,
//! so separately compiled owners cannot reuse each other's sequence numbers.
//! Nothing here is persisted or used as a substitute for a retained run pin.
const std = @import("std");
var next: std.atomic.Value(u64) = .init(1);
pub const Identity = struct {
    issuer: usize = 0,
    sequence: u64 = 0,
    pub fn valid(self: Identity) bool {
        return self.issuer != 0 and self.sequence != 0;
    }
};
pub fn allocate() !Identity {
    return allocateFrom(&next);
}
fn allocateFrom(counter: *std.atomic.Value(u64)) !Identity {
    var observed = counter.load(.monotonic);
    while (true) {
        if (observed == std.math.maxInt(u64)) return error.RunReadIdentityExhausted;
        if (counter.cmpxchgWeak(observed, observed + 1, .monotonic, .monotonic)) |changed| observed = changed else return .{ .issuer = @intFromPtr(counter), .sequence = observed };
    }
}
test "lsm local decoder admission generations never wrap or collide across issuers" {
    var first: std.atomic.Value(u64) = .init(1);
    var second: std.atomic.Value(u64) = .init(1);
    const a = try allocateFrom(&first);
    const b = try allocateFrom(&first);
    const c = try allocateFrom(&second);
    try std.testing.expect(a.valid() and b.valid() and c.valid());
    try std.testing.expect(!std.meta.eql(a, b) and !std.meta.eql(a, c));
    first.store(std.math.maxInt(u64) - 1, .monotonic);
    _ = try allocateFrom(&first);
    try std.testing.expectError(error.RunReadIdentityExhausted, allocateFrom(&first));
    try std.testing.expectError(error.RunReadIdentityExhausted, allocateFrom(&first));
}
