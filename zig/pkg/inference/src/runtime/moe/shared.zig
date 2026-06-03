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

const LayerState = struct {
    num_experts: usize = 0,
    touch_counts: []u64 = &.{},
    coactivation: []u32 = &.{},

    fn deinit(self: *LayerState, allocator: std.mem.Allocator) void {
        allocator.free(self.touch_counts);
        allocator.free(self.coactivation);
        self.* = .{};
    }

    fn ensureExpertCount(self: *LayerState, allocator: std.mem.Allocator, num_experts: usize) !void {
        if (self.num_experts == num_experts) return;
        self.deinit(allocator);
        self.num_experts = num_experts;
        self.touch_counts = try allocator.alloc(u64, num_experts);
        @memset(self.touch_counts, 0);
        self.coactivation = try allocator.alloc(u32, num_experts * num_experts);
        @memset(self.coactivation, 0);
    }

    fn pairScore(self: *const LayerState, lhs: usize, rhs: usize) u32 {
        return self.coactivation[lhs * self.num_experts + rhs];
    }
};

pub const SharedExpertCache = struct {
    allocator: std.mem.Allocator,
    layers: std.ArrayListUnmanaged(LayerState) = .empty,

    pub fn init(allocator: std.mem.Allocator) SharedExpertCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *SharedExpertCache) void {
        for (self.layers.items) |*layer| layer.deinit(self.allocator);
        self.layers.deinit(self.allocator);
        self.* = .{ .allocator = self.allocator };
    }

    pub fn noteActiveExperts(self: *SharedExpertCache, layer_index: usize, num_experts: usize, active_experts: []const u32) !void {
        try self.ensureLayerIndex(layer_index);
        var layer = &self.layers.items[layer_index];
        try layer.ensureExpertCount(self.allocator, num_experts);

        for (active_experts) |expert_index| {
            layer.touch_counts[expert_index] += 1;
        }
        for (active_experts, 0..) |lhs, lhs_idx| {
            for (active_experts[lhs_idx + 1 ..]) |rhs| {
                layer.coactivation[@as(usize, lhs) * num_experts + rhs] += 1;
                layer.coactivation[@as(usize, rhs) * num_experts + lhs] += 1;
            }
        }
    }

    pub fn appendPredictions(
        self: *const SharedExpertCache,
        allocator: std.mem.Allocator,
        layer_index: usize,
        active_experts: []const u32,
        max_predicted: usize,
        out: *std.ArrayListUnmanaged(u32),
        out_scores: *std.ArrayListUnmanaged(u32),
    ) !void {
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
                    score += layer.pairScore(active_expert, candidate);
                }
                score += layer.touch_counts[candidate];
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
