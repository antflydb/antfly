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

//! Minimal node-owned lifetime for remote read uncertainty. Registry tickets
//! own copied cleanup metadata; the destination ledger owns remote attempt
//! credits. This owner keeps their node and original endpoint binding alive,
//! without retaining SQL admission, input, session or foreground cancellation.
const std = @import("std");
const retirement = @import("retained_read_retirement.zig");
const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const dispatch = @import("retained_query_dispatch.zig");
const Dispatcher = dispatch.Dispatcher;
const attempts = @import("../common/remote_attempts.zig");
const Source = @import("remote_endpoint_source.zig").Source;
const View = @import("remote_endpoint_view.zig").View;

pub const Owner = struct {
    allocator: std.mem.Allocator,
    baseline: scheduling.Scheduler.ControlBaseline,
    binding: retirement.Binding = undefined,
    node_pin: @FieldType(retirement.Parent, "pin") = null,
    endpoint_binding: ?*Source.Binding = null,
    endpoint_ref: ?View.Ref = null,
    endpoints_adopted: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    references: usize = 1,
    foreground_live: bool = true,
    detached: std.atomic.Value(bool) = .init(false),

    /// Reserve metadata and node lifetime before publishing any child work.
    pub fn create(allocator: std.mem.Allocator, scheduler: *scheduling.Scheduler, registrar: anytype) !*Owner {
        var baseline = try scheduler.reserveControlBaseline(@sizeOf(Owner));
        errdefer baseline.release();
        const self = try allocator.create(Owner);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .baseline = baseline };
        self.binding = try registrar.bind(.{ .ptr = self, .retain = retain, .release = release });
        const pin = self.binding.parent.pin orelse return error.InvalidRunnableTransition;
        try pin.acquire();
        self.node_pin = pin;
        return self;
    }

    /// Called at setup, before the first ticket or attempt can be published.
    /// The foreground dispatcher remains a synchronous borrow; copied Signed
    /// descriptors instead depend on these independently owned endpoint leases.
    pub fn adoptEndpoints(self: *Owner, dispatcher: *Dispatcher) void {
        std.debug.assert(self.foreground_live and self.references == 1 and !self.endpoints_adopted);
        self.endpoint_binding = dispatcher.endpoint_binding;
        self.endpoint_ref = dispatcher.endpoint_ref;
        dispatcher.endpoint_binding = null;
        dispatcher.endpoint_ref = null;
        self.endpoints_adopted = true;
    }

    pub fn retainedParent(self: *Owner) dispatch.RetainedParent {
        return .{ .parent = self.binding.parent, .context = self, .release = releaseAttempt, .cancellation = &self.detached };
    }
    fn releaseAttempt(raw: *anyopaque, bytes: usize, _: attempts.Id) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const parent = self.binding.parent;
        // This can destroy self. The copied pin is released by Parent.retire.
        parent.retire(bytes);
    }

    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn retain(raw: *anyopaque, _: usize) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.foreground_live or self.detached.load(.acquire) or self.references == std.math.maxInt(usize)) return error.CleanupCapacityUnavailable;
        // Registry count/metadata bytes and destination attempt credits have
        // their own bounded reservations before publication. Do not charge
        // remote-only uncertainty against ordinary local query admission.
        self.references += 1;
    }
    fn release(raw: *anyopaque, _: usize) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.lock();
        std.debug.assert(self.references > @intFromBool(self.foreground_live));
        self.references -= 1;
        const finished = self.references == 0;
        self.mutex.unlock();
        if (finished) self.destroy();
    }

    /// Seal only after all local scans, fences, dispatch callbacks and workers
    /// have joined. Original request admission can then retire independently.
    /// Already published uncertainty remains owned until exact terminal/fence
    /// evidence permits the registry or outbox to release its references.
    pub fn finishForeground(self: *Owner) void {
        self.detached.store(true, .release);
        self.lock();
        std.debug.assert(self.foreground_live and self.references != 0);
        self.foreground_live = false;
        self.references -= 1;
        const finished = self.references == 0;
        self.mutex.unlock();
        if (finished) self.destroy();
    }
    fn destroy(self: *Owner) void {
        const allocator = self.allocator;
        const pin = self.node_pin.?;
        var baseline = self.baseline;
        if (self.endpoint_binding) |binding| binding.release();
        if (self.endpoint_ref) |*ref| ref.release();
        allocator.destroy(self);
        baseline.release();
        pin.retire();
    }
};

const TestNode = struct {
    registry: *retirement.Registry,
    pins: usize = 0,
    refuse: bool = false,
    pub fn bind(self: *@This(), parent: retirement.Parent) !retirement.Binding {
        var bound = parent;
        bound.pin = .{ .ptr = self, .retain = retain, .release = release };
        return .{ .registry = self.registry, .parent = bound };
    }
    fn retain(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.refuse) return error.CleanupCapacityUnavailable;
        self.pins += 1;
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.pins != 0);
        self.pins -= 1;
    }
};

test "remote-only owner retains node through independently ordered registry and outbox retirement" {
    const Resource = struct {
        acknowledged: bool = false,
        destroyed: usize = 0,
        fn poll(raw: *anyopaque, _: retirement.Poll) !retirement.Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.acknowledged) .retired else .pending;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
    };
    for ([_]bool{ false, true }) |outbox_first| {
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .control_bytes = 1 << 20 });
        defer scheduler.close();
        var registry = try retirement.Registry.init(std.testing.allocator, 1, 4096, 4096);
        defer {
            registry.close();
            registry.deinit() catch unreachable;
        }
        var node: TestNode = .{ .registry = &registry };
        const owner = try Owner.create(std.testing.allocator, &scheduler, &node);
        var resource: Resource = .{};
        const ticket = try registry.reserve(owner.binding.parent, 64);
        try ticket.bind(.{ .ptr = &resource, .poll = Resource.poll, .destroy = Resource.destroy });
        try ticket.publish();
        try ticket.transfer();
        const outbox = owner.retainedParent();
        try outbox.parent.acquire(64);
        try std.testing.expectEqual(@as(usize, 3), node.pins);
        owner.finishForeground();
        try std.testing.expect(outbox.cancellation.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, @sizeOf(Owner)), scheduler.control_baseline_bytes);
        try std.testing.expectEqual(scheduler.control_baseline_bytes, scheduler.stats().retained_bytes);
        try std.testing.expectEqual(retirement.Registry.Progress.pending, registry.pollOne(.{ .allocator = std.testing.allocator }));
        // Refusal after sealing must roll back its provisional node pin.
        try std.testing.expectError(error.CleanupCapacityUnavailable, outbox.parent.acquire(64));
        try std.testing.expectEqual(@as(usize, 3), node.pins);
        const id: attempts.Id = .{ .coordinator = 7, .generation = 1, .operation = 3, .sequence = 1 };
        if (outbox_first) outbox.release(outbox.context, 64, id);
        resource.acknowledged = true;
        try std.testing.expectEqual(retirement.Registry.Progress.retired, registry.pollOne(.{ .allocator = std.testing.allocator }));
        try std.testing.expectEqual(@as(usize, 1), resource.destroyed);
        if (!outbox_first) {
            try std.testing.expectEqual(@as(usize, 2), node.pins);
            try std.testing.expect(outbox.cancellation.load(.acquire));
            outbox.release(outbox.context, 64, id);
        }
        try std.testing.expectEqual(@as(usize, 0), node.pins);
        try std.testing.expectEqual(@as(u64, 0), scheduler.control_baseline_bytes);
    }
}

test "remote-only owner creation rolls back metadata baseline when node pin is refused" {
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .control_bytes = 1 << 20 });
    defer scheduler.close();
    var registry = try retirement.Registry.init(std.testing.allocator, 1, 4096, 4096);
    defer {
        registry.close();
        registry.deinit() catch unreachable;
    }
    var node: TestNode = .{ .registry = &registry, .refuse = true };
    try std.testing.expectError(error.CleanupCapacityUnavailable, Owner.create(std.testing.allocator, &scheduler, &node));
    try std.testing.expectEqual(@as(usize, 0), node.pins);
    try std.testing.expectEqual(@as(u64, 0), scheduler.control_baseline_bytes);
}
