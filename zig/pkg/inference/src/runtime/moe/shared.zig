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

const LayerState = struct {
    num_experts: usize = 0,
    touch_counts: []u64 = &.{},
    coactivation: []u32 = &.{},

    pub fn deinit(self: *LayerState, allocator: std.mem.Allocator) void {
        allocator.free(self.touch_counts);
        allocator.free(self.coactivation);
        self.* = .{};
    }

    fn ensureExpertCount(self: *LayerState, allocator: std.mem.Allocator, num_experts: usize) !void {
        if (self.num_experts == num_experts) return;
        const touch_counts = try allocator.alloc(u64, num_experts);
        errdefer allocator.free(touch_counts);
        const coactivation = try allocator.alloc(u32, try std.math.mul(usize, num_experts, num_experts));
        errdefer allocator.free(coactivation);
        @memset(touch_counts, 0);
        @memset(coactivation, 0);
        // Publish only a complete generation; failed resizing retains the old
        // complete statistics and remains safe for a subsequent request.
        self.deinit(allocator);
        self.* = .{ .num_experts = num_experts, .touch_counts = touch_counts, .coactivation = coactivation };
    }

    fn pairScore(self: *const LayerState, lhs: usize, rhs: usize) u32 {
        return self.coactivation[lhs * self.num_experts + rhs];
    }
};

pub const SharedExpertCache = struct {
    allocator: std.mem.Allocator,
    layers: std.ArrayListUnmanaged(LayerState) = .empty,
    mutex: std.atomic.Mutex = .unlocked,
    owned_owner: ?*Owned = null,

    const Owned = struct {
        allocator: std.mem.Allocator,
        references: std.atomic.Value(usize) = .init(1),
        value: SharedExpertCache,

        fn release(self: *Owned) void {
            const previous = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(previous != 0);
            if (previous != 1) return;
            const allocator = self.allocator;
            self.value.deinit();
            allocator.destroy(self);
        }
    };

    pub const Retention = struct {
        owner: *Owned,
        pub fn release(self: Retention) void {
            self.owner.release();
        }
    };

    /// Canonical heap ownership is independent of the model/session and may
    /// outlive the serving aggregate. Stack init/deinit remains a borrowed API.
    pub fn createOwned(allocator: std.mem.Allocator) !*SharedExpertCache {
        const owner = try allocator.create(Owned);
        owner.* = .{ .allocator = allocator, .value = init(allocator) };
        owner.value.owned_owner = owner;
        return &owner.value;
    }

    pub fn retainOwned(self: *SharedExpertCache) ?Retention {
        const owner = self.owned_owner orelse return null;
        const previous = owner.references.fetchAdd(1, .acq_rel);
        std.debug.assert(previous != 0);
        return .{ .owner = owner };
    }

    pub fn releaseOwned(self: *SharedExpertCache) void {
        (self.owned_owner orelse unreachable).release();
    }

    pub fn init(allocator: std.mem.Allocator) SharedExpertCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *SharedExpertCache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.layers.items) |*layer| layer.deinit(self.allocator);
        self.layers.deinit(self.allocator);
        self.layers = .empty;
        self.owned_owner = null;
    }

    pub fn noteActiveExperts(self: *SharedExpertCache, layer_index: usize, num_experts: usize, active_experts: []const u32) !void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try self.ensureLayerIndex(layer_index);
        var layer = &self.layers.items[layer_index];
        try layer.ensureExpertCount(self.allocator, num_experts);

        for (active_experts) |expert_index| {
            layer.touch_counts[expert_index] +|= 1;
        }
        for (active_experts, 0..) |lhs, lhs_idx| {
            for (active_experts[lhs_idx + 1 ..]) |rhs| {
                layer.coactivation[@as(usize, lhs) * num_experts + rhs] +|= 1;
                layer.coactivation[@as(usize, rhs) * num_experts + lhs] +|= 1;
            }
        }
    }

    pub fn appendPredictions(
        self: *SharedExpertCache,
        allocator: std.mem.Allocator,
        layer_index: usize,
        active_experts: []const u32,
        max_predicted: usize,
        out: *std.ArrayListUnmanaged(u32),
        out_scores: *std.ArrayListUnmanaged(u32),
    ) !void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (layer_index >= self.layers.items.len) return;
        const layer = &self.layers.items[layer_index];
        if (layer.num_experts == 0 or active_experts.len == 0 or max_predicted == 0) return;

        var used = try allocator.alloc(bool, layer.num_experts);
        defer allocator.free(used);
        @memset(used, false);
        for (active_experts) |expert_index| used[expert_index] = true;
        for (out.items) |expert_index| used[expert_index] = true;

        while (out.items.len < max_predicted) {
            var best_index: ?usize = null;
            var best_score: u64 = 0;
            for (0..layer.num_experts) |candidate| {
                if (used[candidate]) continue;
                var score: u64 = 0;
                for (active_experts) |active_expert| {
                    score +|= layer.pairScore(active_expert, candidate);
                }
                score +|= layer.touch_counts[candidate];
                if (score > best_score or (best_index == null and score == best_score)) {
                    best_index = candidate;
                    best_score = score;
                }
            }
            const candidate = best_index orelse break;
            if (best_score == 0) break;
            used[candidate] = true;
            try out.append(allocator, @intCast(candidate));
            try out_scores.append(allocator, @intCast(@min(best_score, std.math.maxInt(u32))));
        }
    }

    fn ensureLayerIndex(self: *SharedExpertCache, layer_index: usize) !void {
        while (self.layers.items.len <= layer_index) {
            try self.layers.append(self.allocator, .{});
        }
    }
};

test "shared expert cache predicts from prior requests" {
    const allocator = std.testing.allocator;
    var cache = SharedExpertCache.init(allocator);
    defer cache.deinit();

    try cache.noteActiveExperts(0, 4, &[_]u32{ 1, 2 });
    try cache.noteActiveExperts(0, 4, &[_]u32{ 1, 2 });

    var predicted = std.ArrayListUnmanaged(u32).empty;
    var predicted_scores = std.ArrayListUnmanaged(u32).empty;
    defer predicted.deinit(allocator);
    defer predicted_scores.deinit(allocator);
    try cache.appendPredictions(allocator, 0, &[_]u32{1}, 2, &predicted, &predicted_scores);
    try std.testing.expectEqualSlices(u32, &[_]u32{2}, predicted.items);
    try std.testing.expectEqual(@as(usize, 1), predicted_scores.items.len);
}

test "owned shared expert cache survives model root retirement without owning a session" {
    const a = std.testing.allocator;
    const cache = try SharedExpertCache.createOwned(a);
    var root_open = true;
    defer if (root_open) cache.releaseOwned();
    try cache.noteActiveExperts(0, 4, &.{ 1, 2 });
    const retained = cache.retainOwned().?;
    defer retained.release();
    cache.releaseOwned();
    root_open = false;
    var predicted = std.ArrayListUnmanaged(u32).empty;
    defer predicted.deinit(a);
    var scores = std.ArrayListUnmanaged(u32).empty;
    defer scores.deinit(a);
    try cache.appendPredictions(a, 0, &.{1}, 2, &predicted, &scores);
    try std.testing.expectEqualSlices(u32, &.{2}, predicted.items);
    var borrowed = SharedExpertCache.init(a);
    defer borrowed.deinit();
    try std.testing.expect(borrowed.retainOwned() == null);
}

test "shared expert cache failed resize never publishes incomplete statistics and permits retry" {
    const a = std.testing.allocator;
    for (0..2) |failure_index| {
        var cache = SharedExpertCache.init(a);
        defer cache.deinit();
        try cache.noteActiveExperts(0, 2, &.{ 0, 1 });
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = failure_index });
        cache.allocator = failing.allocator();
        const resize = cache.noteActiveExperts(0, 4, &.{ 1, 2 });
        cache.allocator = a;
        try std.testing.expectError(error.OutOfMemory, resize);
        const state = cache.layers.items[0];
        // Assert bounded metadata before attempting a retry: the old code can
        // otherwise index an empty/partial array after publishing count=4.
        try std.testing.expectEqual(state.num_experts, state.touch_counts.len);
        try std.testing.expectEqual(state.num_experts * state.num_experts, state.coactivation.len);
        try std.testing.expectEqual(@as(usize, 2), state.num_experts);
        try std.testing.expectEqualSlices(u64, &.{ 1, 1 }, state.touch_counts);
        try cache.noteActiveExperts(0, 4, &.{ 1, 2 });
        try std.testing.expectEqual(@as(usize, 4), cache.layers.items[0].touch_counts.len);
    }
}

test "owned shared expert cache serializes concurrent notes and predictions without hot-note allocation" {
    const a = std.testing.allocator;
    const cache = try SharedExpertCache.createOwned(a);
    defer cache.releaseOwned();
    try cache.noteActiveExperts(0, 2, &.{});
    const iterations = 20000;
    const Shared = struct {
        cache: *SharedExpertCache,
        start: std.atomic.Value(bool) = .init(false),
        first_read: std.atomic.Value(bool) = .init(false),
        positive_read: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),
        ready: std.atomic.Value(usize) = .init(0),
        writers: std.atomic.Value(usize) = .init(2),
        reads: std.atomic.Value(usize) = .init(0),
        fn enter(self: *@This()) void {
            _ = self.ready.fetchAdd(1, .acq_rel);
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
        }
        fn note(self: *@This()) void {
            self.enter();
            defer _ = self.writers.fetchSub(1, .acq_rel);
            while (!self.first_read.load(.acquire)) std.atomic.spinLoopHint();
            for (0..iterations) |index| {
                self.cache.noteActiveExperts(0, 2, &.{ 0, 1 }) catch {
                    self.failed.store(true, .release);
                    self.positive_read.store(true, .release);
                    return;
                };
                if (index == iterations / 2) while (!self.positive_read.load(.acquire)) {
                    std.atomic.spinLoopHint();
                };
            }
        }
        fn predict(self: *@This()) void {
            self.enter();
            var out = std.ArrayListUnmanaged(u32).empty;
            defer out.deinit(std.testing.allocator);
            var scores = std.ArrayListUnmanaged(u32).empty;
            defer scores.deinit(std.testing.allocator);
            while (true) {
                out.clearRetainingCapacity();
                scores.clearRetainingCapacity();
                self.cache.appendPredictions(std.testing.allocator, 0, &.{0}, 1, &out, &scores) catch {
                    self.failed.store(true, .release);
                    self.first_read.store(true, .release);
                    self.positive_read.store(true, .release);
                    return;
                };
                _ = self.reads.fetchAdd(1, .acq_rel);
                self.first_read.store(true, .release);
                if (out.items.len != 0) {
                    if (out.items.len != 1 or scores.items.len != 1 or out.items[0] != 1 or scores.items[0] == 0) self.failed.store(true, .release);
                    self.positive_read.store(true, .release);
                }
                if (self.writers.load(.acquire) == 0) return;
            }
        }
    };
    var shared = Shared{ .cache = cache };
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    cache.allocator = failing.allocator();
    var threads: [3]?std.Thread = @splat(null);
    defer {
        shared.start.store(true, .release);
        shared.first_read.store(true, .release);
        shared.positive_read.store(true, .release);
        for (&threads) |*thread| if (thread.*) |worker| {
            worker.join();
            thread.* = null;
        };
        cache.allocator = a;
    }
    threads[0] = try std.Thread.spawn(.{}, Shared.note, .{&shared});
    threads[1] = try std.Thread.spawn(.{}, Shared.note, .{&shared});
    threads[2] = try std.Thread.spawn(.{}, Shared.predict, .{&shared});
    while (shared.ready.load(.acquire) != 3) std.atomic.spinLoopHint();
    shared.start.store(true, .release);
    for (&threads) |*thread| if (thread.*) |worker| {
        worker.join();
        thread.* = null;
    };
    cache.allocator = a;
    try std.testing.expect(!shared.failed.load(.acquire));
    try std.testing.expect(shared.positive_read.load(.acquire));
    try std.testing.expect(shared.reads.load(.acquire) >= 2);
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    // No internal span escapes a cache call. Inspect only after all writers
    // and the actual predictor thread have joined.
    const layer = cache.layers.items[0];
    try std.testing.expectEqualSlices(u64, &.{ 2 * iterations, 2 * iterations }, layer.touch_counts);
    try std.testing.expectEqualSlices(u32, &.{ 0, 2 * iterations, 2 * iterations, 0 }, layer.coactivation);
}

test "shared expert cache hint counters and prediction scores saturate over long uptime" {
    const a = std.testing.allocator;
    var cache = SharedExpertCache.init(a);
    defer cache.deinit();
    try cache.noteActiveExperts(0, 2, &.{});
    const layer = &cache.layers.items[0];
    @memset(layer.touch_counts, std.math.maxInt(u64) - 1);
    layer.coactivation[1] = std.math.maxInt(u32) - 1;
    layer.coactivation[2] = std.math.maxInt(u32) - 1;
    try cache.noteActiveExperts(0, 2, &.{ 0, 1 });
    try cache.noteActiveExperts(0, 2, &.{ 0, 1 });
    try std.testing.expectEqualSlices(u64, &.{ std.math.maxInt(u64), std.math.maxInt(u64) }, layer.touch_counts);
    try std.testing.expectEqual(std.math.maxInt(u32), layer.coactivation[1]);
    try std.testing.expectEqual(std.math.maxInt(u32), layer.coactivation[2]);
    var out = std.ArrayListUnmanaged(u32).empty;
    defer out.deinit(a);
    var scores = std.ArrayListUnmanaged(u32).empty;
    defer scores.deinit(a);
    try cache.appendPredictions(a, 0, &.{0}, 1, &out, &scores);
    try std.testing.expectEqualSlices(u32, &.{1}, out.items);
    try std.testing.expectEqualSlices(u32, &.{std.math.maxInt(u32)}, scores.items);
}
