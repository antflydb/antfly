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

//! A bounded active reader epoch. New pages can replace finished pages while
//! peers continue decoding. The last lease destroys the shared backend state.
const std = @import("std");
const platform = @import("antfly_platform");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Pool = struct {
    pub const max_capacity: usize = 64;
    mutex: std.atomic.Mutex = .unlocked,
    context: ?*anyopaque = null,
    users: usize = 0,
    joins: usize = 0,
    max_epoch_joins: usize = 64,
    destroy_fn: ?*const fn (*anyopaque) void = null,

    pub const Lease = struct {
        owner: *Pool,
        context: *anyopaque,
        pub fn release(self: *Lease) void {
            platform.sync.lockYielding(&self.owner.mutex);
            defer self.owner.mutex.unlock();
            std.debug.assert(self.owner.context == self.context and self.owner.users > 0);
            self.owner.users -= 1;
            if (self.owner.users == 0) {
                self.owner.destroy_fn.?(self.context);
                self.owner.context = null;
                self.owner.destroy_fn = null;
                self.owner.joins = 0;
                self.owner.max_epoch_joins = 64;
            }
        }
    };

    pub fn join(self: *Pool, control: Control, capacity: usize, factory: *anyopaque, create: *const fn (*anyopaque) anyerror!*anyopaque, destroy: *const fn (*anyopaque) void) !Lease {
        return self.joinBounded(control, capacity, 64, factory, create, destroy);
    }

    pub fn joinBounded(self: *Pool, control: Control, capacity: usize, max_epoch_joins: usize, factory: *anyopaque, create: *const fn (*anyopaque) anyerror!*anyopaque, destroy: *const fn (*anyopaque) void) !Lease {
        if (capacity == 0 or capacity > max_capacity or max_epoch_joins < capacity or max_epoch_joins > 256) return error.InvalidReaderCapacity;
        while (true) {
            try control.lock(&self.mutex);
            if (self.context != null and self.max_epoch_joins != max_epoch_joins) {
                self.mutex.unlock();
                return error.InvalidReaderCapacity;
            }
            // Bound each epoch as well as its instantaneous occupancy so a
            // permanent stream cannot monopolize the model execution lane.
            if (self.users < capacity and (self.context == null or self.joins < self.max_epoch_joins)) {
                if (self.context == null) {
                    self.context = create(factory) catch |err| {
                        self.mutex.unlock();
                        return err;
                    };
                    self.destroy_fn = destroy;
                    self.max_epoch_joins = max_epoch_joins;
                }
                self.users += 1;
                self.joins += 1;
                const result = Lease{ .owner = self, .context = self.context.? };
                self.mutex.unlock();
                return result;
            }
            self.mutex.unlock();
            try control.check();
            platform.time.sleepNs(100 * std.time.ns_per_us);
        }
    }
};

test "LightOnOCR3 shared reader pool refills before the longest page retires" {
    const Fake = struct {
        created: usize = 0,
        destroyed: usize = 0,
        fn create(raw: *anyopaque) !*anyopaque {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.created += 1;
            return raw;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
    };
    var fake = Fake{};
    var pool = Pool{};
    var long = try pool.join(.{}, 2, &fake, Fake.create, Fake.destroy);
    var short = try pool.join(.{}, 2, &fake, Fake.create, Fake.destroy);
    short.release();
    for (0..20) |_| {
        var refill = try pool.join(.{}, 2, &fake, Fake.create, Fake.destroy);
        try std.testing.expectEqual(long.context, refill.context);
        refill.release();
    }
    try std.testing.expectEqual(@as(usize, 1), fake.created);
    try std.testing.expectEqual(@as(usize, 0), fake.destroyed);
    long.release();
    try std.testing.expectEqual(@as(usize, 1), fake.destroyed);
    try std.testing.expect(pool.context == null);
    var next = try pool.join(.{}, 2, &fake, Fake.create, Fake.destroy);
    next.release();
    try std.testing.expectEqual(@as(usize, 2), fake.created);
    try std.testing.expectEqual(@as(usize, 2), fake.destroyed);
}

test "LightOnOCR3 shared reader pool rejects canceled joins and unwinds failed creation" {
    const Fake = struct {
        fn create(_: *anyopaque) !*anyopaque {
            return error.OutOfMemory;
        }
        fn destroy(_: *anyopaque) void {
            unreachable;
        }
    };
    var pool = Pool{};
    var value: u8 = 0;
    try std.testing.expectError(error.OutOfMemory, pool.join(.{}, 1, &value, Fake.create, Fake.destroy));
    try std.testing.expect(pool.context == null and pool.users == 0);
    try std.testing.expectError(error.Timeout, pool.join(.{ .deadline_ns = 0 }, 1, &value, Fake.create, Fake.destroy));
}

/// One serialized workspace peak held until the shared runtime is destroyed.
/// The caller serializes growth; each successful increment retains its exact
/// admission lease, so cancellation and later page retirement cannot uncharge
/// a workspace still owned by peers. An explicitly extended epoch accepts at most 256 page joins.
pub fn SharedPeak(comptime Lease: type) type {
    return struct {
        bytes: usize = 0,
        count: usize = 0,
        max_increments: usize = 64,
        leases: [256]Lease = undefined,

        pub fn grow(self: *@This(), target: usize, context: anytype, comptime acquire: anytype) !void {
            if (target <= self.bytes) return;
            if (self.count >= @min(self.max_increments, self.leases.len)) return error.ResourceLimitExceeded;
            const lease = try acquire(context, target - self.bytes);
            self.leases[self.count] = lease;
            self.count += 1;
            self.bytes = target;
        }

        pub fn deinit(self: *@This()) void {
            for (self.leases[0..self.count]) |*lease| lease.release();
            self.bytes = 0;
            self.count = 0;
        }
    };
}

test "LightOnOCR3 shared vision peak preserves caps and reservations across page retirement" {
    const memory = @import("../runtime/tier/memory.zig");
    const Factory = struct {
        fn acquire(controller: *memory.AdmissionController, bytes: usize) !memory.AdmissionLease {
            return controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 100, .scratch_limit_bytes = 100 }, .{ .backend_scratch_bytes = bytes }, false);
        }
    };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var peak = SharedPeak(memory.AdmissionLease){};
    defer peak.deinit();
    try peak.grow(80, &controller, Factory.acquire);
    var first = try Factory.acquire(&controller, 10);
    defer first.release();
    try peak.grow(80, &controller, Factory.acquire);
    var second = try Factory.acquire(&controller, 10);
    defer second.release();
    try std.testing.expectEqual(@as(usize, 100), controller.admitted.backend_scratch_bytes);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, peak.grow(90, &controller, Factory.acquire));
    try std.testing.expectEqual(@as(usize, 80), peak.bytes);
    try std.testing.expectEqual(@as(usize, 1), peak.count);
    first.release();
    try peak.grow(90, &controller, Factory.acquire);
    second.release();
    try std.testing.expectEqual(@as(usize, 90), controller.admitted.backend_scratch_bytes);
    peak.deinit();
    try std.testing.expectEqual(@as(usize, 0), controller.admitted.backend_scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, peak.grow(101, &controller, Factory.acquire));
    try std.testing.expectEqual(@as(usize, 0), peak.count);
    for (1..65) |target| try peak.grow(target, &controller, Factory.acquire);
    try std.testing.expectError(error.ResourceLimitExceeded, peak.grow(65, &controller, Factory.acquire));
}

test "LightOnOCR3 logical window keeps thirty-two independent leases bounded" {
    const Fake = struct {
        created: usize = 0,
        destroyed: usize = 0,
        fn create(raw: *anyopaque) !*anyopaque {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.created += 1;
            return raw;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
    };
    var fake = Fake{};
    var pool = Pool{};
    try std.testing.expectError(error.InvalidReaderCapacity, pool.join(.{}, 65, &fake, Fake.create, Fake.destroy));
    var leases: [32]Pool.Lease = undefined;
    for (&leases) |*lease| lease.* = try pool.join(.{}, 32, &fake, Fake.create, Fake.destroy);
    try std.testing.expectEqual(@as(usize, 32), pool.users);
    try std.testing.expectEqual(@as(usize, 1), fake.created);
    for (1..32) |i| {
        leases[i].release();
        leases[i] = try pool.join(.{}, 32, &fake, Fake.create, Fake.destroy);
        try std.testing.expectEqual(leases[0].context, leases[i].context);
    }
    try std.testing.expectEqual(@as(usize, 0), fake.destroyed);
    for (&leases) |*lease| lease.release();
    try std.testing.expectEqual(@as(usize, 1), fake.destroyed);
    try std.testing.expect(pool.context == null and pool.users == 0);
}

test "LightOnOCR3 logical window keeps sixty-four independent leases bounded" {
    const Fake = struct {
        created: usize = 0,
        destroyed: usize = 0,
        fn create(raw: *anyopaque) !*anyopaque {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.created += 1;
            return raw;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
    };
    var fake = Fake{};
    var pool = Pool{};
    try std.testing.expectError(error.InvalidReaderCapacity, pool.joinBounded(.{}, 65, 256, &fake, Fake.create, Fake.destroy));
    var leases: [64]Pool.Lease = undefined;
    for (&leases) |*lease| lease.* = try pool.joinBounded(.{}, 64, 256, &fake, Fake.create, Fake.destroy);
    try std.testing.expectEqual(@as(usize, 64), pool.users);
    try std.testing.expectEqual(@as(usize, 1), fake.created);
    for (1..64) |i| {
        leases[i].release();
        leases[i] = try pool.joinBounded(.{}, 64, 256, &fake, Fake.create, Fake.destroy);
        try std.testing.expectEqual(leases[0].context, leases[i].context);
    }
    try std.testing.expectEqual(@as(usize, 0), fake.destroyed);
    for (&leases) |*lease| lease.release();
    try std.testing.expectEqual(@as(usize, 1), fake.destroyed);
    try std.testing.expect(pool.context == null and pool.users == 0);
}

test "LightOnOCR3 extended epoch refills beyond sixty-four pages without changing its bound" {
    const Fake = struct {
        created: usize = 0,
        destroyed: usize = 0,
        fn create(raw: *anyopaque) !*anyopaque {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.created += 1;
            return raw;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
    };
    var fake = Fake{};
    var pool = Pool{};
    try std.testing.expectError(error.InvalidReaderCapacity, pool.joinBounded(.{}, 2, 257, &fake, Fake.create, Fake.destroy));
    var long = try pool.joinBounded(.{}, 2, 256, &fake, Fake.create, Fake.destroy);
    for (0..200) |_| {
        var page = try pool.joinBounded(.{}, 2, 256, &fake, Fake.create, Fake.destroy);
        try std.testing.expectEqual(long.context, page.context);
        page.release();
    }
    try std.testing.expectEqual(@as(usize, 201), pool.joins);
    try std.testing.expectEqual(@as(usize, 1), fake.created);
    try std.testing.expectEqual(@as(usize, 0), fake.destroyed);
    try std.testing.expectError(error.InvalidReaderCapacity, pool.join(.{}, 2, &fake, Fake.create, Fake.destroy));
    long.release();
    try std.testing.expectEqual(@as(usize, 1), fake.destroyed);
    var ordinary = try pool.join(.{}, 2, &fake, Fake.create, Fake.destroy);
    try std.testing.expectEqual(@as(usize, 64), pool.max_epoch_joins);
    ordinary.release();
}

test "LightOnOCR3 extended epoch retains every vision increment until teardown" {
    const memory = @import("../runtime/tier/memory.zig");
    const Factory = struct {
        fn acquire(controller: *memory.AdmissionController, bytes: usize) !memory.AdmissionLease {
            return controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 300, .scratch_limit_bytes = 300 }, .{ .backend_scratch_bytes = bytes }, false);
        }
    };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var peak = SharedPeak(memory.AdmissionLease){ .max_increments = 256 };
    defer peak.deinit();
    for (1..257) |target| try peak.grow(target, &controller, Factory.acquire);
    try std.testing.expectEqual(@as(usize, 256), controller.admitted.backend_scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, peak.grow(257, &controller, Factory.acquire));
    try std.testing.expectEqual(@as(usize, 256), peak.bytes);
    peak.deinit();
    try std.testing.expectEqual(@as(usize, 0), controller.admitted.backend_scratch_bytes);
}

test "LightOnOCR3 shared host and device vision peaks preserve the combined scratch cap" {
    const memory = @import("../runtime/tier/memory.zig");
    const Factory = struct {
        fn host(controller: *memory.AdmissionController, bytes: usize) !memory.AdmissionLease {
            return controller.tryAcquire(.gpu, .{ .combined_limit_bytes = 220, .host_limit_bytes = 100, .backend_limit_bytes = 200, .scratch_limit_bytes = 220 }, .{ .host_scratch_bytes = bytes }, false);
        }
        fn device(controller: *memory.AdmissionController, bytes: usize) !memory.AdmissionLease {
            return controller.tryAcquire(.gpu, .{ .combined_limit_bytes = 220, .host_limit_bytes = 100, .backend_limit_bytes = 200, .scratch_limit_bytes = 220 }, .{ .backend_scratch_bytes = bytes }, false);
        }
    };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var host = SharedPeak(memory.AdmissionLease){};
    defer host.deinit();
    var device = SharedPeak(memory.AdmissionLease){};
    defer device.deinit();
    try device.grow(128, &controller, Factory.device);
    try host.grow(64, &controller, Factory.host);
    var page = try Factory.host(&controller, 20);
    defer page.release();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, host.grow(80, &controller, Factory.host));
    try std.testing.expectEqual(@as(usize, 64), host.bytes);
    try std.testing.expectEqual(@as(usize, 1), host.count);
    page.release();
    try host.grow(80, &controller, Factory.host);
    try std.testing.expectEqual(@as(usize, 128), controller.admitted.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 80), controller.admitted.host_scratch_bytes);
    var edge = try Factory.device(&controller, 12);
    defer edge.release();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, device.grow(129, &controller, Factory.device));
    try std.testing.expectEqual(@as(usize, 128), device.bytes);
    edge.release();
    try device.grow(129, &controller, Factory.device);
    device.deinit();
    try std.testing.expectEqual(@as(usize, 80), controller.admitted.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.admitted.backend_scratch_bytes);
    host.deinit();
    try std.testing.expectEqual(@as(usize, 0), controller.admitted.hostTotalBytes() + controller.admitted.backendTotalBytes());
}
