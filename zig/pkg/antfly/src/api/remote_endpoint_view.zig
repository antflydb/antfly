// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Owned target storage. A supervisor owns the initial reference; each query
//! dispatcher holds a separate reference through physical descendant retirement.
//! Publication/refresh must exchange whole views, never mutate a borrowed view.
const std = @import("std");
const Target = @import("remote_attempt_client.zig").Target;
const replies = @import("../common/remote_attempt_reply.zig");
const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub const View = struct {
    allocator: std.mem.Allocator,
    targets: []Target,
    baseline: scheduling.Scheduler.ControlBaseline,
    capabilities: ?*@import("remote_capability_publication.zig").Cache = null,
    references: std.atomic.Value(usize) = .init(1),

    pub fn reservationBytes(descriptors: anytype) !u64 {
        if (descriptors.len == 0 or descriptors.len > 256) return error.InvalidArgument;
        var bytes = try std.math.add(u64, @sizeOf(View), try std.math.mul(u64, descriptors.len, @sizeOf(Target)));
        for (descriptors) |descriptor| {
            if (descriptor.base_uri.len == 0 or descriptor.base_uri.len > 2048) return error.InvalidArgument;
            bytes = try std.math.add(u64, bytes, descriptor.base_uri.len);
        }
        return bytes;
    }
    /// The caller resolves all authorities from one authoritative metadata cut.
    /// Reserve the complete storage before making any allocation visible.
    pub fn create(allocator: std.mem.Allocator, scheduler: *scheduling.Scheduler, descriptors: anytype, authorities: []const replies.Authority) !*View {
        if (authorities.len != descriptors.len) return error.InvalidRemoteMembership;
        for (descriptors, authorities) |descriptor, authority| {
            if (!std.meta.eql(descriptor.local, authority.local)) return error.InvalidRemoteMembership;
        }
        var baseline = scheduler.reserveControlBaseline(try reservationBytes(descriptors)) catch |err| return if (err == error.SchedulerClosed) err else error.CleanupCapacityUnavailable;
        errdefer baseline.release();
        const self = try allocator.create(View);
        errdefer allocator.destroy(self);
        const targets = try allocator.alloc(Target, descriptors.len);
        errdefer allocator.free(targets);
        var initialized: usize = 0;
        errdefer for (targets[0..initialized]) |target| allocator.free(target.base_uri);
        for (targets, descriptors, authorities) |*target, descriptor, authority| {
            target.* = .{ .base_uri = try allocator.dupe(u8, descriptor.base_uri), .authority = authority, .available = if (@hasField(@TypeOf(descriptor), "available")) descriptor.available else true };
            initialized += 1;
        }
        self.* = .{ .allocator = allocator, .targets = targets, .baseline = baseline };
        return self;
    }
    pub fn reservedBytes(self: *const View) u64 {
        return self.baseline.bytes;
    }
    pub fn referenceCount(self: *const View) usize {
        return self.references.load(.acquire);
    }
    pub const Ref = struct {
        view: ?*View,
        pub fn release(self: *Ref) void {
            const view = self.view orelse return;
            self.view = null;
            view.release();
        }
    };
    /// The caller already owns or borrows a reference protected by its supervisor.
    pub fn retain(self: *View) !Ref {
        var count = self.references.load(.acquire);
        while (count != 0 and count != std.math.maxInt(usize)) {
            count = self.references.cmpxchgWeak(count, count + 1, .acq_rel, .acquire) orelse return .{ .view = self };
        }
        return error.InvalidRunnableTransition;
    }
    pub fn release(self: *View) void {
        const count = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(count != 0);
        if (count != 1) return;
        const allocator = self.allocator;
        var baseline = self.baseline;
        if (self.capabilities) |cache| {
            cache.close();
            cache.deinit() catch @panic("endpoint capability probe outlived its physical view lease");
        }
        for (self.targets) |target| allocator.free(target.base_uri);
        allocator.free(self.targets);
        allocator.destroy(self);
        baseline.release();
    }
};

test "remote attempt client endpoint view retains accounted identity storage through last physical pin" {
    const a = std.testing.allocator;
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .control_bytes = 1 << 20 });
    defer node.deinit();
    const local: @import("../common/remote_membership.zig").Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 22, .reporter_incarnation = 17 };
    const authority: replies.Authority = .{ .local = local, .public_key = @splat(7) };
    var uri = "http://worker".*;
    const descriptors = [_]struct { local: @TypeOf(local), base_uri: []const u8 }{.{ .local = local, .base_uri = &uri }};
    const view = try View.create(a, &node.scheduler, &descriptors, &.{authority});
    var owner_live = true;
    defer if (owner_live) view.release();
    var pin = try view.retain();
    defer pin.release();
    const bytes = view.reservedBytes();
    try std.testing.expectEqual(try View.reservationBytes(&descriptors), bytes);
    try std.testing.expectEqual(bytes, node.scheduler.stats().control_retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
    uri[7] = 'x';
    try std.testing.expectEqualStrings("http://worker", view.targets[0].base_uri);
    try std.testing.expectEqualDeep(authority, view.targets[0].authority);
    owner_live = false;
    view.release();
    try std.testing.expectEqual(@as(usize, 1), view.referenceCount());
    try std.testing.expectError(error.InvalidRunnableTransition, node.tryDeinit());
    try std.testing.expectEqual(bytes, node.scheduler.stats().control_retained_bytes);
    pin.release();
    pin.release();
    try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().control_retained_bytes);

    const Failures = struct {
        fn run(allocator: std.mem.Allocator, scheduler: *scheduling.Scheduler, identity: replies.Authority) !void {
            const input = [_]struct { local: @TypeOf(local), base_uri: []const u8 }{.{ .local = identity.local, .base_uri = "http://worker" }};
            const owned = try View.create(allocator, scheduler, &input, &.{identity});
            owned.release();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failures.run, .{ &node.scheduler, authority });
    try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().control_retained_bytes);
    var occupied = try node.scheduler.reserveControlBaseline(node.scheduler.config.control_bytes / 4);
    defer occupied.release();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.CleanupCapacityUnavailable, View.create(failing.allocator(), &node.scheduler, &descriptors, &.{authority}));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}
