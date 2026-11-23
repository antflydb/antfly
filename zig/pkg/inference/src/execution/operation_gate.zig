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

//! Stable, bounded provider metadata for explicit operation tokens. The owner
//! embeds this in its provider and drains actual command-buffer handlers before
//! destroying it. No request allocator, TLS, numerical wrapper or driver policy.
const std = @import("std");

pub const Gate = struct {
    pub const capacity = 64;
    mutex: std.atomic.Mutex = .unlocked,
    next_nonce: std.atomic.Value(u64) = .init(1),
    records: [capacity]Record = @splat(.{}),

    const Record = struct {
        nonce: std.atomic.Value(u64) = .init(0),
        refs: std.atomic.Value(usize) = .init(0),
        owns_gate: std.atomic.Value(bool) = .init(false),
    };

    pub const Ticket = struct {
        owner: *Gate,
        index: usize,
        nonce: u64,
        held: bool = true,

        fn record(self: *const Ticket) !*Record {
            if (!self.held or self.index >= capacity or self.nonce == 0) return error.BackendOperationConsumed;
            const value = &self.owner.records[self.index];
            if (value.nonce.load(.acquire) != self.nonce or value.refs.load(.acquire) == 0) return error.BackendOperationConsumed;
            return value;
        }
        pub fn validate(self: *const Ticket) !void {
            _ = try self.record();
        }
        pub fn ownsGate(self: *const Ticket) !bool {
            return (try self.record()).owns_gate.load(.acquire);
        }
        pub fn begin(self: *const Ticket) !void {
            const value = try self.record();
            if (value.owns_gate.load(.acquire)) return error.BackendOperationConsumed;
            if (!self.owner.mutex.tryLock()) return error.QueueFull;
            value.owns_gate.store(true, .release);
        }
        /// Only the driver owner may call this after its selected terminal or
        /// submission proof. Legacy unknown-frame mode may release after its
        /// synchronous mutations; it cannot grant frozen-region authority.
        pub fn end(self: *const Ticket) !void {
            const value = try self.record();
            if (!value.owns_gate.swap(false, .acq_rel)) return error.BackendOperationInactive;
            self.owner.mutex.unlock();
        }
        /// Actual CBs retain independent Tickets by value. No callback borrows
        /// a caller's stack Lease or dynamically allocated request metadata.
        pub fn retain(self: *const Ticket) !Ticket {
            const value = try self.record();
            var refs = value.refs.load(.acquire);
            while (true) {
                if (refs == 0) return error.BackendOperationConsumed;
                if (refs == std.math.maxInt(usize)) return error.BackendOperationReferenceOverflow;
                if (value.refs.cmpxchgWeak(refs, refs + 1, .acq_rel, .acquire)) |observed| {
                    refs = observed;
                } else return .{ .owner = self.owner, .index = self.index, .nonce = self.nonce };
            }
        }
        pub fn release(self: *Ticket) !void {
            const value = try self.record();
            var refs = value.refs.load(.acquire);
            while (true) {
                if (refs == 0) return error.BackendOperationConsumed;
                if (refs == 1 and value.owns_gate.load(.acquire)) return error.BackendOperationStillActive;
                if (value.refs.cmpxchgWeak(refs, refs - 1, .acq_rel, .acquire)) |observed| {
                    refs = observed;
                } else break;
            }
            self.held = false;
            // Keep nonce occupied until final retirement is finished. Another
            // preparer can reuse this record only after the release store.
            if (refs == 1) value.nonce.store(0, .release);
        }
    };

    pub fn validateNonce(self: *const Gate, index: usize, nonce: u64) !void {
        if (index >= capacity or nonce == 0) return error.BackendOperationConsumed;
        const value = &self.records[index];
        if (value.nonce.load(.acquire) != nonce or value.refs.load(.acquire) == 0) return error.BackendOperationConsumed;
    }
    pub fn prepare(self: *Gate) !Ticket {
        var nonce = self.next_nonce.load(.acquire);
        while (true) {
            if (nonce == 0 or nonce == std.math.maxInt(u64)) return error.BackendOperationIdentityExhausted;
            if (self.next_nonce.cmpxchgWeak(nonce, nonce + 1, .acq_rel, .acquire)) |observed| {
                nonce = observed;
            } else break;
        }
        for (&self.records, 0..) |*value, index| {
            if (value.nonce.cmpxchgStrong(0, nonce, .acq_rel, .acquire) != null) continue;
            std.debug.assert(value.refs.load(.acquire) == 0 and !value.owns_gate.load(.acquire));
            value.refs.store(1, .release);
            return .{ .owner = self, .index = index, .nonce = nonce };
        }
        return error.QueueFull;
    }
    pub fn assertIdle(self: *const Gate) void {
        for (&self.records) |*value| {
            std.debug.assert(value.nonce.load(.acquire) == 0 and value.refs.load(.acquire) == 0 and !value.owns_gate.load(.acquire));
        }
    }
};

test "provider operation tokens preserve completion ownership and reject stale record reuse" {
    var gate = Gate{};
    defer gate.assertIdle();
    var first = try gate.prepare();
    const stale = first;
    try first.begin();
    var independent = try first.retain();
    var blocked = try gate.prepare();
    defer blocked.release() catch unreachable;
    try std.testing.expectError(error.QueueFull, blocked.begin());
    try first.release();
    try std.testing.expect(try independent.ownsGate());
    try std.testing.expectError(error.BackendOperationStillActive, independent.release());
    try independent.end();
    try independent.release();
    var replacement = try gate.prepare();
    defer replacement.release() catch unreachable;
    try std.testing.expectEqual(stale.index, replacement.index);
    try std.testing.expect(stale.nonce != replacement.nonce);
    try std.testing.expectError(error.BackendOperationConsumed, stale.validate());
    try replacement.begin();
    try replacement.end();
}

test "provider operation preparation is bounded and identity exhaustion never wraps" {
    var gate = Gate{};
    defer gate.assertIdle();
    var tickets: [Gate.capacity]Gate.Ticket = undefined;
    var count: usize = 0;
    defer for (tickets[0..count]) |*ticket| ticket.release() catch unreachable;
    while (count != tickets.len) : (count += 1) tickets[count] = try gate.prepare();
    try std.testing.expectError(error.QueueFull, gate.prepare());
    gate.next_nonce.store(std.math.maxInt(u64), .release);
    try std.testing.expectError(error.BackendOperationIdentityExhausted, gate.prepare());
    try std.testing.expectEqual(std.math.maxInt(u64), gate.next_nonce.load(.acquire));
}
