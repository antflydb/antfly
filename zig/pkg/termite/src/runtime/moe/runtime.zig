// Copyright 2026 Antfly, Inc.
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
const shared_mod = @import("shared.zig");

pub const ExpertBatchView = struct {
    rows: []const u32,
    route_weights: []const f32,
};

const ExpertBatchBuffer = struct {
    rows: std.ArrayListUnmanaged(u32) = .empty,
    route_weights: std.ArrayListUnmanaged(f32) = .empty,
    touch_count: u64 = 0,
    last_touched_step: u64 = 0,

    fn deinit(self: *ExpertBatchBuffer, allocator: std.mem.Allocator) void {
        self.rows.deinit(allocator);
        self.route_weights.deinit(allocator);
    }

    fn clear(self: *ExpertBatchBuffer) void {
        self.rows.clearRetainingCapacity();
        self.route_weights.clearRetainingCapacity();
    }
};

const LayerState = struct {
    num_experts: usize = 0,
    expert_batches: []ExpertBatchBuffer = &.{},
    active_flags: []bool = &.{},
    active_experts: std.ArrayListUnmanaged(u32) = .empty,
    predicted_experts: std.ArrayListUnmanaged(u32) = .empty,
    predicted_scores: std.ArrayListUnmanaged(u32) = .empty,
    coactivation: []u32 = &.{},

    fn deinit(self: *LayerState, allocator: std.mem.Allocator) void {
        for (self.expert_batches) |*batch| batch.deinit(allocator);
        allocator.free(self.expert_batches);
        allocator.free(self.active_flags);
        allocator.free(self.coactivation);
        self.active_experts.deinit(allocator);
        self.predicted_experts.deinit(allocator);
        self.predicted_scores.deinit(allocator);
        self.* = .{};
    }

    fn ensureExpertCount(self: *LayerState, allocator: std.mem.Allocator, num_experts: usize) !void {
        if (self.num_experts == num_experts) return;

        self.deinit(allocator);
        self.num_experts = num_experts;
        self.expert_batches = try allocator.alloc(ExpertBatchBuffer, num_experts);
        errdefer allocator.free(self.expert_batches);
        for (self.expert_batches) |*batch| batch.* = .{};
        self.active_flags = try allocator.alloc(bool, num_experts);
        @memset(self.active_flags, false);
        self.coactivation = try allocator.alloc(u32, num_experts * num_experts);
        @memset(self.coactivation, 0);
    }

    fn resetForStep(self: *LayerState) void {
        for (self.active_experts.items) |expert_index| {
            self.expert_batches[expert_index].clear();
            self.active_flags[expert_index] = false;
        }
        self.active_experts.clearRetainingCapacity();
        self.predicted_experts.clearRetainingCapacity();
        self.predicted_scores.clearRetainingCapacity();
    }

    fn pairScore(self: *const LayerState, lhs: usize, rhs: usize) u32 {
        return self.coactivation[lhs * self.num_experts + rhs];
    }
};

pub const MoeRuntime = struct {
    allocator: std.mem.Allocator,
    layers: std.ArrayListUnmanaged(LayerState) = .empty,
    step: u64 = 0,
    cache_hits: u64 = 0,
    cache_misses: u64 = 0,
    shared_cache: ?*shared_mod.SharedExpertCache = null,

    pub fn init(allocator: std.mem.Allocator, shared_cache: ?*shared_mod.SharedExpertCache) MoeRuntime {
        return .{
            .allocator = allocator,
            .shared_cache = shared_cache,
        };
    }

    pub fn deinit(self: *MoeRuntime) void {
        for (self.layers.items) |*layer| layer.deinit(self.allocator);
        self.layers.deinit(self.allocator);
        self.* = .{ .allocator = self.allocator };
    }

    pub fn prepareLayer(self: *MoeRuntime, layer_index: usize, num_experts: usize) !void {
        try self.ensureLayerIndex(layer_index);
        var layer = &self.layers.items[layer_index];
        try layer.ensureExpertCount(self.allocator, num_experts);
        layer.resetForStep();
    }

    pub fn appendRoute(self: *MoeRuntime, layer_index: usize, expert_index: usize, row: u32, route_weight: f32) !void {
        var layer = &self.layers.items[layer_index];
        if (expert_index >= layer.num_experts) return error.InvalidExpertIndex;

        if (!layer.active_flags[expert_index]) {
            layer.active_flags[expert_index] = true;
            try layer.active_experts.append(self.allocator, @intCast(expert_index));
            if (layer.expert_batches[expert_index].touch_count > 0) {
                self.cache_hits += 1;
            } else {
                self.cache_misses += 1;
            }
        }

        try layer.expert_batches[expert_index].rows.append(self.allocator, row);
        try layer.expert_batches[expert_index].route_weights.append(self.allocator, route_weight);
    }

    pub fn finalizeLayer(self: *MoeRuntime, layer_index: usize, max_predicted: usize) !void {
        var layer = &self.layers.items[layer_index];
        self.step += 1;

        for (layer.active_experts.items) |expert_index| {
            layer.expert_batches[expert_index].touch_count += 1;
            layer.expert_batches[expert_index].last_touched_step = self.step;
        }

        for (layer.active_experts.items, 0..) |lhs, lhs_idx| {
            for (layer.active_experts.items[lhs_idx + 1 ..]) |rhs| {
                layer.coactivation[@as(usize, lhs) * layer.num_experts + rhs] += 1;
                layer.coactivation[@as(usize, rhs) * layer.num_experts + lhs] += 1;
            }
        }

        try self.updatePredictions(layer, max_predicted);
        if (self.shared_cache) |shared_cache| {
            try shared_cache.noteActiveExperts(layer_index, layer.num_experts, layer.active_experts.items);
            try shared_cache.appendPredictions(self.allocator, layer_index, layer.active_experts.items, max_predicted, &layer.predicted_experts, &layer.predicted_scores);
        }
    }

    pub fn activeExperts(self: *const MoeRuntime, layer_index: usize) []const u32 {
        return self.layers.items[layer_index].active_experts.items;
    }

    pub fn predictedExperts(self: *const MoeRuntime, layer_index: usize) []const u32 {
        return self.layers.items[layer_index].predicted_experts.items;
    }

    pub fn predictedExpertScores(self: *const MoeRuntime, layer_index: usize) []const u32 {
        return self.layers.items[layer_index].predicted_scores.items;
    }

    pub fn batchView(self: *const MoeRuntime, layer_index: usize, expert_index: usize) ExpertBatchView {
        const batch = &self.layers.items[layer_index].expert_batches[expert_index];
        return .{
            .rows = batch.rows.items,
            .route_weights = batch.route_weights.items,
        };
    }

    fn ensureLayerIndex(self: *MoeRuntime, layer_index: usize) !void {
        while (self.layers.items.len <= layer_index) {
            try self.layers.append(self.allocator, .{});
        }
    }

    fn updatePredictions(self: *MoeRuntime, layer: *LayerState, max_predicted: usize) !void {
        layer.predicted_experts.clearRetainingCapacity();
        layer.predicted_scores.clearRetainingCapacity();
        if (max_predicted == 0 or layer.active_experts.items.len == 0) return;

        var used = try self.allocator.alloc(bool, layer.num_experts);
        defer self.allocator.free(used);
        @memset(used, false);
        for (layer.active_experts.items) |expert_index| used[expert_index] = true;

        const limit = @min(max_predicted, layer.num_experts -| layer.active_experts.items.len);
        for (0..limit) |_| {
            var best_index: ?usize = null;
            var best_score: u32 = 0;
            for (0..layer.num_experts) |candidate| {
                if (used[candidate]) continue;
                var score: u32 = 0;
                for (layer.active_experts.items) |active_expert| {
                    score += layer.pairScore(active_expert, candidate);
                }
                if (score > best_score or (best_index == null and score == best_score)) {
                    best_index = candidate;
                    best_score = score;
                }
            }
            const candidate = best_index orelse break;
            if (best_score == 0) break;
            used[candidate] = true;
            try layer.predicted_experts.append(self.allocator, @intCast(candidate));
            try layer.predicted_scores.append(self.allocator, best_score);
        }
    }
};

test "moe runtime reuses active buffers and predicts co-activations" {
    const allocator = std.testing.allocator;
    var runtime = MoeRuntime.init(allocator, null);
    defer runtime.deinit();

    try runtime.prepareLayer(0, 4);
    try runtime.appendRoute(0, 1, 0, 0.6);
    try runtime.appendRoute(0, 2, 0, 0.4);
    try runtime.finalizeLayer(0, 2);
    try std.testing.expectEqual(@as(u64, 0), runtime.cache_hits);
    try std.testing.expectEqual(@as(u64, 2), runtime.cache_misses);

    try runtime.prepareLayer(0, 4);
    try runtime.appendRoute(0, 1, 1, 0.7);
    try runtime.appendRoute(0, 2, 1, 0.3);
    try runtime.finalizeLayer(0, 2);
    try std.testing.expectEqual(@as(u64, 2), runtime.cache_hits);
    try std.testing.expectEqual(@as(usize, 2), runtime.activeExperts(0).len);
    const batch = runtime.batchView(0, 1);
    try std.testing.expectEqual(@as(usize, 1), batch.rows.len);

    try runtime.prepareLayer(0, 4);
    try runtime.appendRoute(0, 1, 2, 0.7);
    try runtime.finalizeLayer(0, 2);
    try std.testing.expectEqualSlices(u32, &[_]u32{2}, runtime.predictedExperts(0));
    try std.testing.expectEqual(@as(usize, 1), runtime.predictedExpertScores(0).len);
    try std.testing.expect(runtime.predictedExpertScores(0)[0] > 0);
}

test "moe runtime consults shared cache" {
    const allocator = std.testing.allocator;
    var shared_cache = shared_mod.SharedExpertCache.init(allocator);
    defer shared_cache.deinit();
    try shared_cache.noteActiveExperts(0, 4, &[_]u32{ 1, 3 });

    var runtime = MoeRuntime.init(allocator, &shared_cache);
    defer runtime.deinit();

    try runtime.prepareLayer(0, 4);
    try runtime.appendRoute(0, 1, 0, 1.0);
    try runtime.finalizeLayer(0, 2);
    try std.testing.expectEqualSlices(u32, &[_]u32{3}, runtime.predictedExperts(0));
    try std.testing.expectEqual(@as(usize, 1), runtime.predictedExpertScores(0).len);
}
