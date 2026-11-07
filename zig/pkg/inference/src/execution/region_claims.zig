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

//! Move-only pre-admission payload claims. Warm preparation borrows a marked
//! Frame's preallocated array; cold preparation owns temporary metadata only.
//! No numerical work or memory credit can be created by this owner.
const std = @import("std");
const region = @import("region_route.zig");

pub const Set = struct {
    owner_identity: usize,
    epoch: u64,
    claims: []?region.PayloadClaim,
    allocator: ?std.mem.Allocator = null,
    closed: bool = false,

    pub const Metadata = struct {
        allocator: std.mem.Allocator,
        values: []?region.PayloadClaim,
        pub fn deinit(self: Metadata) void {
            self.allocator.free(self.values);
        }
    };
    pub fn borrowed(owner: usize, epoch: u64, claims: []?region.PayloadClaim) !Set {
        if (epoch == 0) return error.InvalidPreparedRegionClaim;
        for (claims) |maybe| if (maybe) |claim| if (claim.held) return error.PreparedRegionClaimsBusy;
        @memset(claims, null);
        return .{ .owner_identity = owner, .epoch = epoch, .claims = claims };
    }
    pub fn create(allocator: std.mem.Allocator, owner: usize, epoch: u64, count: usize) !Set {
        const claims = try allocator.alloc(?region.PayloadClaim, count);
        errdefer allocator.free(claims);
        @memset(claims, null);
        var result = try borrowed(owner, epoch, claims);
        result.allocator = allocator;
        return result;
    }
    /// Only before submission (quota rejection/cancellation). Backend release
    /// updates metadata/refcounts; checked cache eviction owns driver frees.
    pub fn releaseUnsubmitted(self: *Set) void {
        if (self.closed) return;
        self.closed = true;
        for (self.claims) |*maybe| {
            if (maybe.*) |*claim| claim.release();
            maybe.* = null;
        }
        if (self.allocator) |allocator| allocator.free(self.claims);
        self.claims = &.{};
    }
    /// Callbacks/acquisition occur outside Pool lock. After checking the stable
    /// Frame claim mark, this commit moves values only. Returned metadata must
    /// be freed AFTER unlocking. Mismatches retain original pin ownership.
    pub fn moveToFrame(self: *Set, owner: usize, epoch: u64, destination: []?region.PayloadClaim) !?Metadata {
        if (self.closed) return error.PreparedRegionClaimConsumed;
        if (owner != self.owner_identity or epoch != self.epoch or destination.len != self.claims.len) return error.InvalidPreparedRegionClaim;
        var retired: ?Metadata = null;
        if (destination.ptr != self.claims.ptr) {
            if (self.allocator == null) return error.InvalidPreparedRegionClaim;
            for (destination) |maybe| if (maybe) |claim| if (claim.held) return error.PreparedRegionClaimsBusy;
            for (self.claims, destination) |*source, *target| {
                target.* = source.*;
                source.* = null;
            }
            retired = .{ .allocator = self.allocator.?, .values = self.claims };
        } else if (self.allocator != null) return error.InvalidPreparedRegionClaim;
        self.claims = &.{};
        self.closed = true;
        return retired;
    }
};

pub const Acquisition = union(enum) { no_regions, ready: Set, reprepare };

const Fixture = struct {
    references: usize = 1,
    fn retain(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.references += 1;
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.references -= 1;
    }
    fn validate(_: *anyopaque, _: region.Validate) !void {}
    fn transfer(_: *anyopaque, _: region.Submission) !void {
        return error.UnexpectedSubmission;
    }
    fn drain(_: *anyopaque) !void {
        return error.UnexpectedDrain;
    }
    const table = region.PayloadClaim.VTable{ .validate = validate, .transfer_to_submission = transfer, .drain = drain };
    fn claim(self: *@This()) region.PayloadClaim {
        return .{ .resource = .{ .context = self, .retain_fn = retain, .release_fn = release }, .key = 7, .vtable = &table };
    }
    fn check(allocator: std.mem.Allocator) !void {
        var fixture = Fixture{};
        var pending = try Set.create(allocator, 11, 13, 2);
        defer pending.releaseUnsubmitted();
        pending.claims[0] = fixture.claim();
        var destination: [2]?region.PayloadClaim = @splat(null);
        try std.testing.expectError(error.InvalidPreparedRegionClaim, pending.moveToFrame(12, 13, &destination));
        try std.testing.expectEqual(@as(usize, 1), fixture.references);
        const metadata = try pending.moveToFrame(11, 13, &destination);
        defer if (metadata) |old| old.deinit();
        try std.testing.expect(metadata != null and pending.closed);
        try std.testing.expectEqual(@as(usize, 1), fixture.references);
        try std.testing.expectError(error.PreparedRegionClaimConsumed, pending.moveToFrame(11, 13, &destination));
        destination[0].?.release();
        try std.testing.expectEqual(@as(usize, 0), fixture.references);
    }
};

test "region payload claims move exactly once without driver work or under-lock allocator cleanup" {
    try Fixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
    var storage: [1]?region.PayloadClaim = @splat(null);
    var other: [1]?region.PayloadClaim = @splat(null);
    var fixture = Fixture{};
    var borrowed = try Set.borrowed(1, 2, &storage);
    borrowed.claims[0] = fixture.claim();
    try std.testing.expectError(error.InvalidPreparedRegionClaim, borrowed.moveToFrame(1, 2, &other));
    borrowed.releaseUnsubmitted();
    try std.testing.expectEqual(@as(usize, 0), fixture.references);
    try std.testing.expect(storage[0] == null);
}
