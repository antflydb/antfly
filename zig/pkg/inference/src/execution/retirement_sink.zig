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

//! Independent CPU retirement metadata. An epoch becomes eligible only after
//! its actual outer provider/model gate has ended; later epochs cannot postpone
//! that retirement. Ref pins metadata, not a model, provider, IO, or permission
//! to execute. Boundaries borrow a retained Epoch and never infer current work.
const std = @import("std");
const retirement = @import("resource_retirement.zig");
const platform = @import("antfly_platform");
const metadata_allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());

const Owner = struct {
    const capacity = 64;
    allocator: std.mem.Allocator,
    identity: u64,
    references: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    closed: bool = false,
    next_nonce: u64 = 1,
    records: [capacity]Record = @splat(.{}),

    const Record = struct {
        owner: *Owner = undefined,
        nonce: u64 = 0,
        references: usize = 0,
        sealed: bool = false,
        guards: usize = 0,
        head: ?*retirement.Node = null,
        tail: ?*retirement.Node = null,
    };
    fn lock(self: *Owner) void {
        platform.sync.lockYielding(&self.mutex);
    }
    fn retain(self: *Owner) void {
        var count = self.references.load(.monotonic);
        while (true) {
            if (count == 0 or count == std.math.maxInt(usize)) @panic("invalid retirement sink reference");
            count = self.references.cmpxchgWeak(count, count + 1, .monotonic, .monotonic) orelse return;
        }
    }
    fn release(self: *Owner) void {
        const old = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(old != 0);
        if (old != 1) return;
        for (self.records) |record| std.debug.assert(record.references == 0 and record.head == null and record.tail == null);
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};

/// Owned stable sink pin. Copies require retain; release consumes the value.
pub const Ref = struct {
    owner: ?*Owner,
    identity: u64,

    /// Requested process payload for one independent cold outer owner.
    pub fn requestedMetadataBytes() usize {
        return @sizeOf(Owner);
    }
    pub fn create() !Ref {
        return createWithAllocator(metadata_allocator);
    }
    fn createWithAllocator(allocator: std.mem.Allocator) !Ref {
        const owner = try allocator.create(Owner);
        owner.* = .{ .allocator = allocator, .identity = @import("source_identity.zig").next() };
        for (&owner.records) |*record| record.owner = owner;
        return .{ .owner = owner, .identity = owner.identity };
    }
    fn get(self: Ref) !*Owner {
        const owner = self.owner orelse return error.RetirementSinkConsumed;
        if (self.identity == 0 or owner.identity != self.identity) return error.RetirementSinkConsumed;
        return owner;
    }
    pub fn retain(self: Ref) !Ref {
        const owner = try self.get();
        owner.retain();
        return self;
    }
    pub fn release(self: *Ref) void {
        const owner = self.owner orelse return;
        std.debug.assert(owner.identity == self.identity);
        self.owner = null;
        self.identity = 0;
        owner.release();
    }
    /// Prevent future outer epochs; existing retained epochs remain valid for
    /// late GPU-ready cleanup after the original owner has gone away.
    pub fn close(self: Ref) !void {
        const owner = try self.get();
        owner.lock();
        defer owner.mutex.unlock();
        owner.closed = true;
    }
    /// Metadata reservation only. Caller couples this guard to the actual
    /// outer gate and finishes it AFTER that gate/mutex has been released.
    pub fn enter(self: Ref) !Guard {
        const owner = try self.get();
        owner.lock();
        defer owner.mutex.unlock();
        if (owner.closed) return error.RetirementSinkClosed;
        if (owner.next_nonce == 0 or owner.next_nonce == std.math.maxInt(u64)) return error.RetirementEpochIdentityExhausted;
        for (&owner.records) |*record| {
            if (record.references != 0) continue;
            std.debug.assert(record.head == null and record.tail == null);
            record.nonce = owner.next_nonce;
            owner.next_nonce += 1;
            record.references = 1;
            record.sealed = false;
            record.guards = 1;
            owner.retain();
            return .{ .epoch = .{ .record = record, .nonce = record.nonce } };
        }
        return error.RetirementEpochCapacityExceeded;
    }
};

/// Owned exact epoch. Retain this value when handing a boundary to asynchronous
/// ownership. A stable Ref alone does not identify an actual cleanup boundary.
pub const Epoch = struct {
    record: ?*Owner.Record,
    nonce: u64,

    fn validateLocked(self: Epoch) !*Owner.Record {
        const record = self.record orelse return error.RetirementEpochConsumed;
        if (self.nonce == 0 or record.nonce != self.nonce or record.references == 0) return error.RetirementEpochConsumed;
        return record;
    }
    /// Pure origin attestation, not cleanup authority. Keep the stable sink
    /// alive without occupying one of its bounded epoch records. A record can
    /// be reused only after its previous nonce was sealed and released.
    pub fn sealWitness(self: Epoch) !SealWitness {
        const record = self.record orelse return error.RetirementEpochConsumed;
        const owner = record.owner;
        owner.lock();
        defer owner.mutex.unlock();
        _ = try self.validateLocked();
        owner.retain();
        return .{ .sink = .{ .owner = owner, .identity = owner.identity }, .record = record, .nonce = self.nonce };
    }
    pub fn retain(self: Epoch) !Epoch {
        const record = self.record orelse return error.RetirementEpochConsumed;
        const owner = record.owner;
        owner.lock();
        defer owner.mutex.unlock();
        _ = try self.validateLocked();
        if (record.references == std.math.maxInt(usize)) return error.RetirementEpochReferenceOverflow;
        record.references += 1;
        owner.retain();
        return self;
    }
    pub fn release(self: *Epoch) void {
        const record = self.record orelse return;
        const owner = record.owner;
        owner.lock();
        _ = self.validateLocked() catch @panic("invalid retirement epoch release");
        std.debug.assert(record.references > 1 or record.sealed);
        record.references -= 1;
        if (record.references == 0) {
            std.debug.assert(record.head == null and record.tail == null);
            // Retain the last sealed generation for pure SealWitness readers.
            // Zero references already revoke every Epoch/boundary capability;
            // enter() publishes a strictly newer nonce when reusing this slot.
            std.debug.assert(record.nonce != 0 and record.sealed);
        }
        owner.mutex.unlock();
        self.record = null;
        self.nonce = 0;
        owner.release();
    }
    pub fn isSealed(self: Epoch) !bool {
        const record = self.record orelse return error.RetirementEpochConsumed;
        record.owner.lock();
        defer record.owner.mutex.unlock();
        _ = try self.validateLocked();
        return record.sealed;
    }
    pub fn boundary(self: Epoch) !retirement.Charge.DeferredBoundary {
        const record = self.record orelse return error.RetirementEpochConsumed;
        record.owner.lock();
        defer record.owner.mutex.unlock();
        _ = try self.validateLocked();
        return .{ .context = record, .nonce = self.nonce, .register = register };
    }
    /// Intrusive ownership publication which must not run a callback inline
    /// under the caller's metadata mutex. Sealing and registration are checked
    /// atomically by this SAME actual epoch owner; existing boundary() retains
    /// its original completed-epoch immediate cleanup behavior.
    pub fn boundaryBeforeSeal(self: Epoch) !retirement.Charge.DeferredBoundary {
        var result = try self.boundary();
        result.register = registerBeforeSeal;
        return result;
    }
    fn registerBeforeSeal(raw: *anyopaque, nonce: u64, node: *retirement.Node) !void {
        return registerImpl(raw, nonce, node, false);
    }
    fn register(raw: *anyopaque, nonce: u64, node: *retirement.Node) !void {
        return registerImpl(raw, nonce, node, true);
    }
    fn registerImpl(raw: *anyopaque, nonce: u64, node: *retirement.Node, allow_sealed: bool) !void {
        const record: *Owner.Record = @ptrCast(@alignCast(raw));
        const owner = record.owner;
        owner.lock();
        var held = Epoch{ .record = record, .nonce = nonce };
        _ = held.validateLocked() catch |err| {
            owner.mutex.unlock();
            return err;
        };
        // Intrusive nodes are caller-owned and unregistered until success.
        if (node.next != null or record.tail == node) {
            owner.mutex.unlock();
            return error.RetirementNodeAlreadyRegistered;
        }
        if (!record.sealed) {
            if (record.tail) |tail| tail.next = node else record.head = node;
            record.tail = node;
            owner.mutex.unlock();
            return;
        }
        if (!allow_sealed) {
            owner.mutex.unlock();
            return error.RetirementEpochSealed;
        }
        // A callback may release the last external epoch/sink pin or reenter
        // registration. Keep this record and owner alive until it returns.
        if (record.references == std.math.maxInt(usize)) {
            owner.mutex.unlock();
            return error.RetirementEpochReferenceOverflow;
        }
        record.references += 1;
        owner.retain();
        owner.mutex.unlock();
        node.release_fn(node.context);
        held.release();
    }
};

/// A completed-origin witness owns only process metadata. It never supplies an
/// Epoch, DeferredBoundary, operation token or numerical target. Reading under
/// the SAME sink mutex makes both sealing and monotonic record reuse race-free;
/// exhausted nonces are rejected before enter() can publish another generation.
pub const SealWitness = struct {
    sink: Ref,
    record: *Owner.Record,
    nonce: u64,
    pub fn wasSealed(self: *const SealWitness) !bool {
        const owner = try self.sink.get();
        owner.lock();
        defer owner.mutex.unlock();
        if (self.record.owner != owner or self.nonce == 0 or self.record.nonce < self.nonce) return error.InvalidRetirementSealWitness;
        return self.record.nonce > self.nonce or self.record.sealed;
    }
    pub fn release(self: *SealWitness) void {
        self.sink.release();
    }
};

pub const Guard = struct {
    epoch: Epoch,
    held: bool = true,

    pub fn retainEpoch(self: *const Guard) !Epoch {
        if (!self.held) return error.RetirementEpochConsumed;
        return self.epoch.retain();
    }
    pub fn boundary(self: *const Guard) !retirement.Charge.DeferredBoundary {
        if (!self.held) return error.RetirementEpochConsumed;
        return self.epoch.boundary();
    }
    /// A separately owned part of this SAME actual cleanup boundary. The
    /// caller must couple each fork to a concrete provider/model owner; this
    /// never waits for later or unrelated sink epochs. No allocation occurs.
    pub fn fork(self: *const Guard) !Guard {
        if (!self.held) return error.RetirementEpochConsumed;
        const record = self.epoch.record orelse return error.RetirementEpochConsumed;
        const owner = record.owner;
        owner.lock();
        defer owner.mutex.unlock();
        _ = try self.epoch.validateLocked();
        if (record.sealed or record.guards == 0) return error.RetirementEpochSealed;
        if (record.references == std.math.maxInt(usize) or record.guards == std.math.maxInt(usize)) return error.RetirementEpochReferenceOverflow;
        record.references += 1;
        record.guards += 1;
        owner.retain();
        return .{ .epoch = self.epoch };
    }
    /// Caller has already ended the relevant actual provider AND outer model
    /// gate. No callback executes under this metadata mutex or a future epoch.
    pub fn finish(self: *Guard) void {
        if (!self.held) return;
        const record = self.epoch.record.?;
        const owner = record.owner;
        owner.lock();
        _ = self.epoch.validateLocked() catch @panic("invalid retirement guard");
        std.debug.assert(!record.sealed and record.guards != 0);
        record.guards -= 1;
        self.held = false;
        if (record.guards != 0) {
            owner.mutex.unlock();
            self.epoch.release();
            return;
        }
        record.sealed = true;
        var nodes = record.head;
        record.head = null;
        record.tail = null;
        self.held = false;
        owner.mutex.unlock();
        while (nodes) |node| {
            nodes = node.next;
            node.next = null;
            node.release_fn(node.context);
        }
        self.epoch.release();
    }
};

const Counter = struct {
    calls: usize = 0,
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *Counter = @ptrCast(@alignCast(raw));
        self.calls += 1;
    }
    fn node(self: *Counter) retirement.Node {
        return .{ .context = self, .release_fn = release };
    }
};

test "retirement sink seals exact epochs without waiting for later outer gates" {
    var sink = try Ref.createWithAllocator(std.testing.allocator);
    defer sink.release();
    var first = try sink.enter();
    defer first.finish();
    var second = try sink.enter();
    defer second.finish();
    var retained = try first.retainEpoch();
    defer retained.release();
    var counter = Counter{};
    var a = counter.node();
    var b = counter.node();
    var late = counter.node();
    const old_boundary = try first.boundary();
    const next_boundary = try second.boundary();
    try old_boundary.register(old_boundary.context, old_boundary.nonce, &a);
    try next_boundary.register(next_boundary.context, next_boundary.nonce, &b);
    try std.testing.expectEqual(@as(usize, 0), counter.calls);
    try std.testing.expectError(error.RetirementNodeAlreadyRegistered, old_boundary.register(old_boundary.context, old_boundary.nonce, &a));
    try sink.close();
    try std.testing.expectError(error.RetirementSinkClosed, sink.enter());
    first.finish();
    try std.testing.expectEqual(@as(usize, 1), counter.calls);
    try std.testing.expect(try retained.isSealed());
    const late_boundary = try retained.boundary();
    try late_boundary.register(late_boundary.context, late_boundary.nonce, &late);
    try std.testing.expectEqual(@as(usize, 2), counter.calls);
    try std.testing.expect(!try second.epoch.isSealed());
    second.finish();
    try std.testing.expectEqual(@as(usize, 3), counter.calls);
}

test "retirement sink rejects stale reused epoch nonce and exhausts bounded metadata honestly" {
    var sink = try Ref.createWithAllocator(std.testing.allocator);
    defer sink.release();
    var guards: [Owner.capacity]Guard = undefined;
    var count: usize = 0;
    defer for (guards[0..count]) |*guard| guard.finish();
    while (count < guards.len) : (count += 1) guards[count] = try sink.enter();
    try std.testing.expectError(error.RetirementEpochCapacityExceeded, sink.enter());
    const stale = try guards[0].boundary();
    guards[0].finish();
    var replacement = try sink.enter();
    defer replacement.finish();
    const current = try replacement.boundary();
    try std.testing.expect(stale.context == current.context and stale.nonce != current.nonce);
    var counter = Counter{};
    var node = counter.node();
    try std.testing.expectError(error.RetirementEpochConsumed, stale.register(stale.context, stale.nonce, &node));
    try current.register(current.context, current.nonce, &node);
    replacement.finish();
    try std.testing.expectEqual(@as(usize, 1), counter.calls);
    sink.owner.?.next_nonce = std.math.maxInt(u64);
    try std.testing.expectError(error.RetirementEpochIdentityExhausted, sink.enter());
}

test "retirement sink callbacks reenter only after actual outer unlock and outside metadata mutex" {
    var sink = try Ref.createWithAllocator(std.testing.allocator);
    defer sink.release();
    var provider = std.atomic.Mutex.unlocked;
    var guard = try sink.enter();
    defer guard.finish();
    try std.testing.expect(provider.tryLock());
    var provider_held = true;
    defer if (provider_held) provider.unlock();
    const Callback = struct {
        sink: *Ref,
        provider: *std.atomic.Mutex,
        called: bool = false,
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.provider.tryLock()) @panic("retirement under provider gate");
            self.provider.unlock();
            var nested = self.sink.enter() catch @panic("retirement held metadata mutex");
            nested.finish();
            self.called = true;
        }
    };
    var callback = Callback{ .sink = &sink, .provider = &provider };
    var node = retirement.Node{ .context = &callback, .release_fn = Callback.release };
    const boundary = try guard.boundary();
    try boundary.register(boundary.context, boundary.nonce, &node);
    try std.testing.expect(!callback.called);
    provider.unlock();
    provider_held = false;
    guard.finish();
    try std.testing.expect(callback.called);
}

test "retirement sink retained epoch accepts concurrent late cleanup after original owner closes" {
    if (comptime @import("builtin").single_threaded or @import("build_options").enable_wasm) return error.SkipZigTest;
    var sink = try Ref.createWithAllocator(std.testing.allocator);
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var counter = std.atomic.Value(usize).init(0);
    var start = std.atomic.Value(bool).init(false);
    var nodes: [4][32]retirement.Node = undefined;
    var threads: [4]?std.Thread = @splat(null);
    defer {
        start.store(true, .release);
        for (&threads) |*thread| if (thread.*) |value| {
            value.join();
            thread.* = null;
        };
    }
    const Worker = struct {
        fn release(raw: *anyopaque) callconv(.c) void {
            const total: *std.atomic.Value(usize) = @ptrCast(@alignCast(raw));
            _ = total.fetchAdd(1, .monotonic);
        }
        fn run(owned: Epoch, slots: *[32]retirement.Node, total: *std.atomic.Value(usize), signal: *std.atomic.Value(bool)) void {
            var epoch = owned;
            defer epoch.release();
            while (!signal.load(.acquire)) std.atomic.spinLoopHint();
            const boundary = epoch.boundary() catch @panic("lost retained retirement epoch");
            for (slots) |*node| {
                node.* = .{ .context = total, .release_fn = release };
                boundary.register(boundary.context, boundary.nonce, node) catch @panic("late retirement registration failed");
            }
        }
    };
    for (0..threads.len) |index| {
        var epoch = try guard.retainEpoch();
        threads[index] = std.Thread.spawn(.{}, Worker.run, .{ epoch, &nodes[index], &counter, &start }) catch |err| {
            epoch.release();
            return err;
        };
    }
    guard.finish();
    try sink.close();
    sink.release();
    start.store(true, .release);
    for (&threads) |*thread| if (thread.*) |value| {
        value.join();
        thread.* = null;
    };
    try std.testing.expectEqual(@as(usize, 128), counter.load(.acquire));
}

fn allocationCase(allocator: std.mem.Allocator) !void {
    var sink = try Ref.createWithAllocator(allocator);
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var counter = Counter{};
    var node = counter.node();
    const boundary = try epoch.boundary();
    try boundary.register(boundary.context, boundary.nonce, &node);
    try sink.close();
    guard.finish();
    try std.testing.expectEqual(@as(usize, 1), counter.calls);
}

test "retirement sink construction and epochs unwind every allocation failure" {
    try allocationCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}

test "retirement guard forks wait for exact provider and outer unlock in either order" {
    for ([_]bool{ false, true }) |provider_first| {
        var owner = try Ref.create();
        defer owner.release();
        var outer = try owner.enter();
        defer outer.finish();
        var provider = try outer.fork();
        defer provider.finish();
        var epoch = try outer.retainEpoch();
        defer epoch.release();
        var count = Counter{};
        var node = retirement.Node{ .context = &count, .release_fn = Counter.release };
        const b = try epoch.boundary();
        try b.register(b.context, b.nonce, &node);
        if (provider_first) provider.finish() else outer.finish();
        try std.testing.expect(!try epoch.isSealed());
        try std.testing.expectEqual(@as(usize, 0), count.calls);
        if (provider_first) outer.finish() else provider.finish();
        try std.testing.expect(try epoch.isSealed());
        try std.testing.expectEqual(@as(usize, 1), count.calls);
        try std.testing.expectError(error.RetirementEpochConsumed, outer.fork());
        // A new actual owner cannot postpone retirement of the old epoch.
        var next = try owner.enter();
        defer next.finish();
        try std.testing.expectEqual(@as(usize, 1), count.calls);
    }
}

test "retirement origin seal witness survives record reuse without pinning its old epoch" {
    var sink = try Ref.createWithAllocator(std.testing.allocator);
    var first = try sink.enter();
    var old = try first.epoch.sealWitness();
    defer old.release();
    try std.testing.expect(!try old.wasSealed());
    const record = first.epoch.record;
    const stale = first.epoch;
    first.finish();
    try std.testing.expect(try old.wasSealed());
    try std.testing.expectError(error.RetirementEpochConsumed, stale.boundary());
    try std.testing.expectError(error.RetirementEpochConsumed, stale.retain());
    try std.testing.expectError(error.RetirementEpochConsumed, stale.sealWitness());
    var second = try sink.enter();
    try std.testing.expect(second.epoch.record == record);
    try std.testing.expect(second.epoch.nonce > old.nonce);
    var current = try second.epoch.sealWitness();
    defer current.release();
    try std.testing.expect(try old.wasSealed());
    try std.testing.expect(!try current.wasSealed());
    second.finish();
    try std.testing.expect(try current.wasSealed());
    try sink.close();
    sink.release(); // Witnesses independently retain pure metadata afterwards.
    try std.testing.expect(try old.wasSealed());
    try std.testing.expect(try current.wasSealed());
}

test "retirement origin seal witness neither allocates nor accepts stale or foreign generation" {
    var allocator = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var sink = try Ref.createWithAllocator(allocator.allocator());
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var witness = try guard.epoch.sealWitness();
    defer witness.release();
    try std.testing.expect(!try witness.wasSealed());
    try std.testing.expectError(error.OutOfMemory, Ref.createWithAllocator(allocator.allocator()));
    const stale = Epoch{ .record = guard.epoch.record, .nonce = guard.epoch.nonce + 1 };
    try std.testing.expectError(error.RetirementEpochConsumed, stale.sealWitness());
    var another = try Ref.createWithAllocator(std.testing.allocator);
    defer another.release();
    var foreign = witness;
    foreign.sink = another;
    try std.testing.expectError(error.InvalidRetirementSealWitness, foreign.wasSealed());
}

test "retirement origin seal witness observes concurrent seal and refuses nonce exhaustion as progress" {
    const Observer = struct {
        witness: *const SealWitness,
        started: std.atomic.Value(bool) = .init(false),
        observed: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.started.store(true, .release);
            const deadline = platform.time.monotonicNs() + std.time.ns_per_s;
            while (platform.time.monotonicNs() < deadline) {
                const sealed = self.witness.wasSealed() catch {
                    self.failed.store(true, .release);
                    return;
                };
                if (sealed) {
                    self.observed.store(true, .release);
                    return;
                }
                platform.time.sleepNs(100 * std.time.ns_per_us);
            }
            self.failed.store(true, .release);
        }
    };
    var sink = try Ref.createWithAllocator(std.testing.allocator);
    defer sink.release();
    var first = try sink.enter();
    var witness = try first.epoch.sealWitness();
    defer witness.release();
    const next = sink.owner.?.next_nonce;
    sink.owner.?.next_nonce = std.math.maxInt(u64);
    try std.testing.expectError(error.RetirementEpochIdentityExhausted, sink.enter());
    try std.testing.expect(!try witness.wasSealed());
    sink.owner.?.next_nonce = next;
    var observer = Observer{ .witness = &witness };
    const thread = try std.Thread.spawn(.{}, Observer.run, .{&observer});
    while (!observer.started.load(.acquire)) platform.time.sleepNs(100 * std.time.ns_per_us);
    first.finish();
    var reused = try sink.enter();
    defer reused.finish();
    thread.join();
    try std.testing.expect(!observer.failed.load(.acquire));
    try std.testing.expect(observer.observed.load(.acquire));
}

test "copied owned baseline identity survives actual sink destruction and address reuse" {
    var bytes: [@sizeOf(Owner)]u8 align(@alignOf(Owner)) = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&bytes);
    var registry: @import("allocation_registry.zig").Registry = undefined;
    registry.begin(std.testing.allocator);
    defer {
        if (registry.attached) _ = registry.finish();
    }
    var first = try Ref.createWithAllocator(fixed.allocator());
    const old_address = first.owner;
    const old_identity = first.identity;
    @import("allocation_registry.zig").bindPreexistingOwnedPayload(.{ .identity = old_identity, .host_scratch_payload_bytes = 17 });
    first.release();
    var second = try Ref.createWithAllocator(fixed.allocator());
    try std.testing.expectEqual(old_address, second.owner);
    try std.testing.expect(old_identity != second.identity and second.identity != 0);
    @import("allocation_registry.zig").bindPreexistingOwnedPayload(.{ .identity = second.identity, .host_scratch_payload_bytes = 19 });
    second.release();
    const result = registry.finish();
    try std.testing.expectEqual(@as(u8, 2), result.preexisting_owned_payloads.count);
    try std.testing.expectEqual(old_identity, result.preexisting_owned_payloads.records[0].identity);
    try std.testing.expectEqual(@as(u64, 17), result.preexisting_owned_payloads.records[0].host_scratch_payload_bytes);
}

test "pre-seal boundary registration rejects completed epoch without inline callback" {
    const Probe = struct {
        calls: usize = 0,
        fn ended(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
    };
    var sink = try Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    const before_seal = try epoch.boundaryBeforeSeal();
    var probe: Probe = .{};
    var first = retirement.Node{ .context = &probe, .release_fn = Probe.ended };
    try before_seal.register(before_seal.context, before_seal.nonce, &first);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    guard.finish();
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    var rejected = retirement.Node{ .context = &probe, .release_fn = Probe.ended };
    try std.testing.expectError(error.RetirementEpochSealed, before_seal.register(before_seal.context, before_seal.nonce, &rejected));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    // Existing completed-boundary contract is unchanged and still immediate.
    const ordinary = try epoch.boundary();
    try ordinary.register(ordinary.context, ordinary.nonce, &rejected);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
}
