// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

/// Allocation ownership, independent of how many immutable roots reference it.
/// Each charged allocation and each generation handle keeps the account alive.
pub const Account = struct {
    backing: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    bytes: std.atomic.Value(u64) = .init(0),
    last_pass: u64 = 0,

    pub fn create(backing: std.mem.Allocator) !*Account {
        const self = try backing.create(Account);
        self.* = .{ .backing = backing };
        self.bytes.store(@sizeOf(Account), .monotonic);
        return self;
    }
    pub fn retain(self: *Account) *Account {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *Account) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) self.backing.destroy(self);
    }
    pub fn charge(self: *Account, bytes: usize) void {
        _ = self.retain();
        _ = self.bytes.fetchAdd(bytes, .monotonic);
    }
    pub fn discharge(self: *Account, bytes: usize) void {
        _ = self.bytes.fetchSub(bytes, .monotonic);
        self.release();
    }
    /// The owning backend serializes accounting passes and generation changes.
    pub fn chargeOnce(self: *Account, pass: u64) u64 {
        if (self.last_pass == pass) return 0;
        self.last_pass = pass;
        return self.bytes.load(.acquire);
    }
};

var pass_id: std.atomic.Value(u64) = .init(1);
pub fn nextPass() u64 {
    return pass_id.fetchAdd(1, .monotonic);
}
