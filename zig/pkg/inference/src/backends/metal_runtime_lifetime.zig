// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pure endpoint liveness metadata. It owns no runtime, SDK object, Model,
//! numerical operation or cleanup boundary. A user may touch its independently
//! borrowed runtime endpoint only while beginUse succeeds. This never supplies
//! numerical/reuse authority or replaces the caller's explicit CURRENT cleanup.
const std = @import("std");
const platform = @import("antfly_platform");

pub const Lifetime = struct {
    allocator: std.mem.Allocator,
    identity: u64,
    endpoint_address: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    references: usize = 1,
    users: usize = 0,
    open: bool = true,

    pub fn prepare() !*Lifetime {
        return prepareWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()));
    }
    fn prepareWithAllocator(a: std.mem.Allocator) !*Lifetime {
        const self = try a.create(Lifetime);
        self.* = .{ .allocator = a, .identity = @import("../execution/source_identity.zig").next() };
        return self;
    }
    fn lock(self: *Lifetime) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    /// Caller already owns a metadata reference (provider or exact Root).
    pub fn retain(self: *Lifetime) *Lifetime {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.references != 0 and self.references != std.math.maxInt(usize));
        self.references += 1;
        return self;
    }
    /// Cold immutable address identity only, not an SDK owner/capture. No late
    /// pure callback dereferences this number. Actual users supply their own
    /// borrowed address, which must match before any runtime access.
    pub fn bindEndpointAddress(self: *Lifetime, address: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.endpoint_address == 0 and self.users == 0 and self.open);
        self.endpoint_address = address;
    }
    pub fn beginUseFor(self: *Lifetime, address: usize) bool {
        self.lock();
        defer self.mutex.unlock();
        if (!self.open or address == 0 or self.endpoint_address != address) return false;
        std.debug.assert(self.users != std.math.maxInt(usize));
        self.users += 1;
        return true;
    }
    pub fn beginUse(self: *Lifetime) bool {
        self.lock();
        defer self.mutex.unlock();
        if (!self.open) return false;
        std.debug.assert(self.users != std.math.maxInt(usize));
        self.users += 1;
        return true;
    }
    pub fn endUse(self: *Lifetime) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.users != 0);
        self.users -= 1;
    }
    /// Actual unique provider owner calls this BEFORE freeing its endpoint.
    /// A user holds no metadata mutex while touching SDK/retirement locks.
    /// Consequently close cannot invert SDK-lock -> lifetime-lock ordering.
    pub fn closeAndDrain(self: *Lifetime) void {
        self.lock();
        self.open = false;
        while (self.users != 0) {
            self.mutex.unlock();
            std.Thread.yield() catch {};
            self.lock();
        }
        self.mutex.unlock();
    }
    pub fn release(self: *Lifetime) void {
        self.lock();
        std.debug.assert(self.references != 0);
        self.references -= 1;
        if (self.references != 0) {
            self.mutex.unlock();
            return;
        }
        std.debug.assert(!self.open and self.users == 0);
        const a = self.allocator;
        self.mutex.unlock();
        a.destroy(self);
    }
};

test "Metal endpoint metadata survives provider close but grants no postclose access" {
    const lifetime = try Lifetime.prepareWithAllocator(std.testing.allocator);
    lifetime.bindEndpointAddress(17);
    const root = lifetime.retain();
    try std.testing.expect(!root.beginUseFor(18));
    try std.testing.expect(root.beginUseFor(17));
    root.endUse();
    try std.testing.expect(lifetime.beginUse());
    lifetime.endUse();
    lifetime.closeAndDrain();
    lifetime.release(); // actual provider/header can now disappear
    try std.testing.expect(!root.beginUse());
    try std.testing.expect(!root.beginUseFor(17));
    root.release(); // pure last metadata only, no SDK/Model callback
}

test "Metal endpoint close drains actual concurrent use before returning" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const lifetime = try Lifetime.prepareWithAllocator(std.testing.allocator);
    defer {
        lifetime.closeAndDrain();
        lifetime.release();
    }
    const Worker = struct {
        endpoint: *Lifetime,
        started: std.atomic.Value(bool) = .init(false),
        finished: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.endpoint.closeAndDrain();
            self.finished.store(true, .release);
        }
    };
    var worker: Worker = .{ .endpoint = lifetime };
    try std.testing.expect(lifetime.beginUse());
    var in_use = true;
    defer if (in_use) lifetime.endUse();
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    defer {
        if (in_use) {
            lifetime.endUse();
            in_use = false;
        }
        thread.join();
    }
    while (!worker.started.load(.acquire)) std.atomic.spinLoopHint();
    // Wait for the close transition itself, rather than trusting a latch that
    // merely precedes entry. No new reader may enter after the transition.
    while (lifetime.beginUse()) lifetime.endUse();
    try std.testing.expect(!worker.finished.load(.acquire));
    lifetime.endUse();
    in_use = false;
    while (!worker.finished.load(.acquire)) std.atomic.spinLoopHint();
}

test "Metal endpoint metadata OOM publishes no runtime owner" {
    var allocator = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Lifetime.prepareWithAllocator(allocator.allocator()));
}
