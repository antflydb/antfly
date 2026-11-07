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
const Snapshot = @import("antfly_local_sources").api_table_read_source.RelationalStatementSnapshot;

/// Session candidates share an immutable cut. Last-release only retires it;
/// remote close callbacks run outside registry mutexes and record stripes.
pub const Retirement = struct {
    head: std.atomic.Value(?*Shared) = .init(null),

    pub fn drain(self: *Retirement) void {
        var next = self.head.swap(null, .acq_rel);
        while (next) |cut| {
            next = cut.next;
            cut.snapshot.deinit();
            cut.alloc.destroy(cut);
        }
    }
};

pub const Shared = struct {
    alloc: std.mem.Allocator,
    snapshot: Snapshot,
    retirement: *Retirement,
    refs: std.atomic.Value(usize) = .init(1),
    next: ?*Shared = null,
    lost: std.atomic.Value(bool) = .init(false),

    /// Consumes the snapshot only on success.
    pub fn create(alloc: std.mem.Allocator, snapshot: Snapshot, retirement: *Retirement) !*Shared {
        const self = try alloc.create(Shared);
        self.* = .{ .alloc = alloc, .snapshot = snapshot, .retirement = retirement };
        return self;
    }
    pub fn retain(self: *Shared) *Shared {
        const prior = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(prior != 0 and prior != std.math.maxInt(usize));
        return self;
    }
    pub fn renew(self: *Shared, deadline_ns: ?u64) !void {
        if (self.lost.load(.acquire)) return error.RetainedReadRestartRequired;
        self.snapshot.renew(deadline_ns) catch |err| {
            self.lost.store(true, .release);
            return err;
        };
    }
    pub fn release(self: *Shared) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        var head = self.retirement.head.load(.acquire);
        while (true) {
            self.next = head;
            head = self.retirement.head.cmpxchgWeak(head, self, .release, .acquire) orelse return;
        }
    }
};

test "SQL retained snapshots retire outside registry locks and close exactly once" {
    const Fixture = struct {
        closes: usize = 0,
        renewals: usize = 0,
        fn renew(ptr: *anyopaque, _: ?u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.renewals += 1;
            return error.ConnectionResetByPeer;
        }
        fn open(_: *anyopaque, _: std.mem.Allocator, _: @import("antfly_local_sources").api_table_read_source.RelationalStatementScan) !@import("antfly_local_sources").api_table_read_source.RelationalReadView {
            return error.TestUnexpectedResult;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
    };
    var fixture: Fixture = .{};
    var retirement: Retirement = .{};
    const snapshot: Snapshot = .{ .ptr = &fixture, .vtable = &.{ .renew = Fixture.renew, .open = Fixture.open, .close = Fixture.close } };
    const first = try Shared.create(std.testing.allocator, snapshot, &retirement);
    const candidate = first.retain();
    try std.testing.expectError(error.ConnectionResetByPeer, first.renew(null));
    try std.testing.expectError(error.RetainedReadRestartRequired, candidate.renew(null));
    try std.testing.expectEqual(1, fixture.renewals);
    first.release();
    retirement.drain();
    try std.testing.expectEqual(0, fixture.closes);
    candidate.release();
    try std.testing.expectEqual(0, fixture.closes);
    retirement.drain();
    retirement.drain();
    try std.testing.expectEqual(1, fixture.closes);
}

test "SQL retained snapshot certification preserves capture fences and accepts completed history" {
    const Proof = @import("../raft/mod.zig").ReadSafetyBarrier.FrozenProof;
    const frozen: Proof = .{ .incarnation = 7, .term = 3, .applied_index = 10 };
    try frozen.validate(7, 3, true, 10);
    try std.testing.expectError(error.ReadUnavailable, frozen.validate(7, 3, true, 11));
    try frozen.validateRetained(.{ .incarnation = 7, .term = 3, .applied_index = 11 });
    try std.testing.expectError(error.NotLeader, frozen.validateRetained(.{ .incarnation = 8, .term = 3, .applied_index = 11 }));
    try std.testing.expectError(error.NotLeader, frozen.validateRetained(.{ .incarnation = 7, .term = 4, .applied_index = 11 }));
    try std.testing.expectError(error.ReadUnavailable, frozen.validateRetained(.{ .incarnation = 7, .term = 3, .applied_index = 9 }));
}
