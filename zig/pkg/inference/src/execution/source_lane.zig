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

//! Lexical proof of an actually acquired model-owned source lane. Callers must
//! first qualify the mutex against the retained ModelLifetime's execution_gate.
//! Borrowed values are synchronous only and cannot outlive the Guard storage.
const std = @import("std");
const identities = @import("source_identity.zig");
pub const Identity = struct {
    context: *anyopaque,
    table: *const anyopaque,
    pub fn eql(a: Identity, b: Identity) bool {
        return a.context == b.context and a.table == b.table;
    }
};
pub const Borrowed = struct {
    owner: *const Guard,
    nonce: u64,
    pub fn validate(self: Borrowed, expected: Identity) !void {
        // This is a lexical capability, not a weak cross-thread handle. The
        // original owner must remain live throughout this synchronous call.
        if (!self.owner.held or self.owner.nonce != self.nonce or
            !self.owner.identity.eql(expected)) return error.SourceLaneAuthorityExpired;
    }
};
pub const Guard = struct {
    lane: ?*std.atomic.Mutex = null,
    identity: Identity = undefined,
    nonce: u64 = 0,
    held: bool = false,
    /// Initialize in final caller storage; never return a Guard by value once
    /// any borrowed proof has been issued. Acquisition cannot publish a proof
    /// before the actual mutex is held. No allocator or background metadata.
    pub fn acquireInto(qualified_lane: *std.atomic.Mutex, identity: Identity, control: ?@import("../execution_control.zig").InferenceExecutionControl, output: *Guard) !void {
        if (output.held) return error.SourceLaneAlreadyHeld;
        if (control) |active| try active.lock(qualified_lane) else while (!qualified_lane.tryLock()) std.atomic.spinLoopHint();
        output.* = .{ .lane = qualified_lane, .identity = identity, .nonce = identities.next(), .held = true };
    }
    pub fn borrow(self: *const Guard) !Borrowed {
        if (!self.held) return error.SourceLaneAuthorityExpired;
        return .{ .owner = self, .nonce = self.nonce };
    }
    pub fn finish(self: *Guard) void {
        if (!self.held) return;
        const lane = self.lane.?;
        // Invalidate BEFORE actual lane release. A later acquisition in the
        // same stack storage cannot revive any previous borrowed proof.
        self.held = false;
        self.nonce = 0;
        self.lane = null;
        lane.unlock();
    }
};
test "source lane proof expires before unlock and never revives on stack reuse" {
    var lane: std.atomic.Mutex = .unlocked;
    var context: u8 = 0;
    var table: u8 = 0;
    const identity = Identity{ .context = &context, .table = &table };
    var guard = Guard{};
    try Guard.acquireInto(&lane, identity, null, &guard);
    const old = try guard.borrow();
    try old.validate(identity);
    var other: u8 = 0;
    try std.testing.expectError(error.SourceLaneAuthorityExpired, old.validate(.{ .context = &other, .table = &table }));
    guard.finish();
    try std.testing.expectError(error.SourceLaneAuthorityExpired, old.validate(identity));
    try Guard.acquireInto(&lane, identity, null, &guard);
    defer guard.finish();
    try std.testing.expectError(error.SourceLaneAuthorityExpired, old.validate(identity));
    try (try guard.borrow()).validate(identity);
}

test "source lane cancelled acquisition publishes no proof and leaves mutex available" {
    var lane: std.atomic.Mutex = .unlocked;
    var context: u8 = 0;
    var table: u8 = 0;
    var guard = Guard{};
    try std.testing.expectError(error.Timeout, Guard.acquireInto(&lane, .{ .context = &context, .table = &table }, .{ .deadline_ns = 0 }, &guard));
    try std.testing.expectError(error.SourceLaneAuthorityExpired, guard.borrow());
    try std.testing.expect(lane.tryLock());
    lane.unlock();
}
