// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Model-owned, fixed-capacity prototype cache. Filling slots are reserved
//! before execution, never evicted, and single-flight for an identical key.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const scoring = @import("antfly_decisions").scoring;
pub const capacity = 256;
pub const admitted_bytes = 1024 * 1024;

const Slot = struct {
    state: enum { empty, filling, ready } = .empty,
    key: [32]u8 = @splat(0),
    generation: u64 = 0,
    touched: u64 = 0,
    dimensions: usize = 0,
    values: [768]f32 = undefined,
};

pub const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    slots: [capacity]Slot = @splat(.{}),
    clock: u64 = 0,
    hits: usize = 0,
    builds: usize = 0,
    waits: usize = 0,
    evictions: usize = 0,

    pub const Claim = union(enum) { hit: []f32, owner: Ticket, bypass };
    pub const Ticket = struct {
        cache: *Cache,
        index: usize,
        generation: u64,

        pub fn abort(self: Ticket) void {
            @import("antfly_platform").sync.lockYielding(&self.cache.mutex);
            defer self.cache.mutex.unlock();
            const slot = &self.cache.slots[self.index];
            if (slot.generation == self.generation and slot.state == .filling) slot.state = .empty;
        }

        pub fn publish(self: Ticket, values: []const f32) !void {
            for (values) |v| if (!std.math.isFinite(v)) return error.InvalidEmbeddingDecisionOutput;
            @import("antfly_platform").sync.lockYielding(&self.cache.mutex);
            defer self.cache.mutex.unlock();
            const slot = &self.cache.slots[self.index];
            if (slot.generation != self.generation or slot.state != .filling or values.len != slot.dimensions) return error.InvalidEmbeddingDecisionOutput;
            @memcpy(slot.values[0..values.len], values);
            slot.state = .ready;
            self.cache.builds += 1;
        }
    };

    pub fn claim(self: *Cache, a: std.mem.Allocator, io: std.Io, k: [32]u8, dimensions: usize, control: Control) !Claim {
        if (!@import("../architectures/embedding_gemma2.zig").validDimension(dimensions)) return error.InvalidEmbeddingDimensions;
        while (true) {
            try control.check();
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            var pending = false;
            for (&self.slots) |*slot| {
                if (slot.state != .empty and std.mem.eql(u8, &slot.key, &k)) {
                    if (slot.state == .filling) {
                        pending = true;
                        self.waits += 1;
                        break;
                    }
                    if (slot.dimensions != dimensions) {
                        self.mutex.unlock();
                        return error.InvalidEmbeddingDecisionOutput;
                    }
                    const copy = a.dupe(f32, slot.values[0..dimensions]) catch |err| {
                        self.mutex.unlock();
                        return err;
                    };
                    self.clock +%= 1;
                    slot.touched = self.clock;
                    self.hits += 1;
                    self.mutex.unlock();
                    return .{ .hit = copy };
                }
            }
            if (pending) {
                self.mutex.unlock();
                // Waiters own no slot pointers or backend/model locks. A
                // cancelled waiter cannot cancel the owner's publication.
                try io.sleep(.fromMilliseconds(5), .awake);
                continue;
            }
            var candidate: ?usize = null;
            for (self.slots, 0..) |slot, i| {
                if (slot.state == .empty) {
                    candidate = i;
                    break;
                }
                if (slot.state == .ready and (candidate == null or slot.touched < self.slots[candidate.?].touched)) candidate = i;
            }
            const index = candidate orelse {
                self.mutex.unlock();
                return .bypass;
            };
            const slot = &self.slots[index];
            if (slot.state == .ready) self.evictions += 1;
            self.clock +%= 1;
            slot.* = .{ .state = .filling, .key = k, .generation = self.clock, .touched = self.clock, .dimensions = dimensions };
            const generation = slot.generation;
            self.mutex.unlock();
            return .{ .owner = .{ .cache = self, .index = index, .generation = generation } };
        }
    }
};

comptime {
    std.debug.assert(@sizeOf(Cache) <= admitted_bytes);
}

pub fn key(task: []const u8, dimensions: usize, inputs: []const []const u8) [32]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    h.update(scoring.renderer_version);
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, dimensions, .little);
    h.update(&bytes);
    std.mem.writeInt(u64, &bytes, task.len, .little);
    h.update(&bytes);
    h.update(task);
    for (inputs) |input| {
        std.mem.writeInt(u64, &bytes, input.len, .little);
        h.update(&bytes);
        h.update(input);
    }
    return h.finalResult();
}

pub fn prototypeSetHash(a: std.mem.Allocator, question: anytype, options: scoring.Options, mode: []const u8) ![64]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("prototype-set-v1");
    hash.update(mode);
    for (question.labels, question.descriptions, 0..) |label, description, index| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, label.len, .little);
        hash.update(&length);
        hash.update(label);
        const examples = if (question.examples.len == 0) &.{} else question.examples[index];
        const rendered = try a.alloc([]const u8, @max(examples.len, 1));
        defer a.free(rendered);
        var count: usize = 0;
        defer for (rendered[0..count]) |value| a.free(value);
        for (rendered, 0..) |*value, i| {
            value.* = if (examples.len == 0) try scoring.renderCategory(a, question.instructions, description) else try scoring.renderInput(a, question.instructions, examples[i]);
            count += 1;
        }
        hash.update(&key(options.task_type, options.dimensions, rendered));
    }
    return std.fmt.bytesToHex(hash.finalResult(), .lower);
}

test "embeddinggemma2 prototype reservations bound capacity and survive aborted owners" {
    const a = std.testing.allocator;
    const cache = try a.create(Cache);
    defer a.destroy(cache);
    cache.* = .{};
    var values: [128]f32 = @splat(0);
    values[0] = 1;
    const k = key("CLUSTERING", 128, &.{"first"});
    const first = try cache.claim(a, std.testing.io, k, 128, .{});
    first.owner.abort();
    const retry = try cache.claim(a, std.testing.io, k, 128, .{});
    try retry.owner.publish(&values);
    const hit = try cache.claim(a, std.testing.io, k, 128, .{});
    defer a.free(hit.hit);
    try std.testing.expectEqualSlices(f32, &values, hit.hit);
    for (0..capacity + 1) |i| {
        var numbered: [8]u8 = undefined;
        std.mem.writeInt(u64, &numbered, i, .little);
        const claim = try cache.claim(a, std.testing.io, key("CLUSTERING", 128, &.{&numbered}), 128, .{});
        switch (claim) {
            .owner => |owner| try owner.publish(&values),
            .hit => |v| a.free(v),
            .bypass => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expect(cache.evictions > 0);
    try std.testing.expect(!std.mem.eql(u8, &key("CLUSTERING", 128, &.{ "ab", "c" }), &key("CLUSTERING", 128, &.{ "a", "bc" })));
}

test "embeddinggemma2 concurrent prototype waiters coalesce and cancel independently" {
    const a = std.testing.allocator;
    const cache = try a.create(Cache);
    defer a.destroy(cache);
    cache.* = .{};
    const k = key("CLUSTERING", 128, &.{"shared"});
    const owner = (try cache.claim(a, std.testing.io, k, 128, .{})).owner;
    defer owner.abort();
    const Worker = struct {
        cache: *Cache,
        k: [32]u8,
        cancelled: std.atomic.Value(bool) = .init(false),
        result: ?anyerror = null,
        hit: bool = false,
        fn canceled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.cancelled.load(.acquire);
        }
        fn run(self: *@This()) void {
            const claim = self.cache.claim(std.heap.smp_allocator, std.testing.io, self.k, 128, .{ .cancellation = .{ .ptr = self, .is_cancelled_fn = canceled } }) catch |err| {
                self.result = err;
                return;
            };
            switch (claim) {
                .hit => |values| {
                    std.heap.smp_allocator.free(values);
                    self.hit = true;
                },
                else => self.result = error.UnexpectedCacheOwner,
            }
        }
    };
    var first = Worker{ .cache = cache, .k = k };
    var second = Worker{ .cache = cache, .k = k };
    const thread1 = try std.Thread.spawn(.{}, Worker.run, .{&first});
    var joined1 = false;
    defer if (!joined1) {
        first.cancelled.store(true, .release);
        thread1.join();
    };
    const thread2 = try std.Thread.spawn(.{}, Worker.run, .{&second});
    var joined2 = false;
    defer if (!joined2) {
        second.cancelled.store(true, .release);
        thread2.join();
    };
    const deadline = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
    while (true) {
        @import("antfly_platform").sync.lockYielding(&cache.mutex);
        const waiting = cache.waits >= 2;
        cache.mutex.unlock();
        if (waiting) break;
        if (@import("antfly_platform").time.monotonicNs() > deadline) return error.CacheWaiterTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    first.cancelled.store(true, .release);
    thread1.join();
    joined1 = true;
    // The first join is complete; avoid a second join on a later error.
    var values: [128]f32 = @splat(0);
    values[0] = 1;
    owner.publish(&values) catch unreachable;
    thread2.join();
    joined2 = true;
    try std.testing.expectEqual(error.Cancelled, first.result.?);
    try std.testing.expect(second.result == null and second.hit);
    try std.testing.expectEqual(@as(usize, 1), cache.builds);
}
