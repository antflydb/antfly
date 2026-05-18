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

pub const ExpertCoord = struct {
    layer_index: usize,
    expert_index: u32,
};

pub const ExpertPriority = struct {
    touch_count: u64 = 0,
    last_touched_step: u64 = 0,
};

const ExpertState = struct {
    touch_count: u64 = 0,
    last_touched_step: u64 = 0,
    resident_projection_mask: u8 = 0,
    resident_projection_count: u8 = 0,
    resident_bytes: usize = 0,
};

const LayerState = struct {
    experts: []ExpertState = &.{},

    fn deinit(self: *LayerState, allocator: std.mem.Allocator) void {
        allocator.free(self.experts);
        self.* = .{};
    }

    fn ensureExpertCount(self: *LayerState, allocator: std.mem.Allocator, num_experts: usize) !void {
        if (self.experts.len == num_experts) return;
        self.deinit(allocator);
        self.experts = try allocator.alloc(ExpertState, num_experts);
        @memset(self.experts, .{});
    }

    fn residentExpertCount(self: *const LayerState) usize {
        var count: usize = 0;
        for (self.experts) |expert| {
            if (expert.resident_projection_count > 0) count += 1;
        }
        return count;
    }
};

pub const SharedResidency = struct {
    allocator: std.mem.Allocator,
    max_resident_experts_per_layer: usize,
    layers: std.ArrayListUnmanaged(LayerState) = .empty,
    step: u64 = 0,
    resident_bytes: usize = 0,
    total_projection_loads: u64 = 0,
    total_projection_evictions: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, max_resident_experts_per_layer: usize) SharedResidency {
        return .{
            .allocator = allocator,
            .max_resident_experts_per_layer = max_resident_experts_per_layer,
        };
    }

    pub fn deinit(self: *SharedResidency) void {
        for (self.layers.items) |*layer| layer.deinit(self.allocator);
        self.layers.deinit(self.allocator);
        self.* = .{
            .allocator = self.allocator,
            .max_resident_experts_per_layer = self.max_resident_experts_per_layer,
        };
    }

    pub fn noteTouch(self: *SharedResidency, coord: ExpertCoord, num_experts: usize) !void {
        try self.ensureExpert(coord.layer_index, num_experts);
        self.step += 1;
        var expert = &self.layers.items[coord.layer_index].experts[coord.expert_index];
        expert.touch_count += 1;
        expert.last_touched_step = self.step;
    }

    pub fn noteLoad(self: *SharedResidency, coord: ExpertCoord, num_experts: usize, projection_mask: u8, resident_bytes: usize) !void {
        try self.ensureExpert(coord.layer_index, num_experts);
        var expert = &self.layers.items[coord.layer_index].experts[coord.expert_index];
        if ((expert.resident_projection_mask & projection_mask) != 0) return;
        expert.resident_projection_mask |= projection_mask;
        expert.resident_projection_count += 1;
        expert.resident_bytes += resident_bytes;
        self.resident_bytes += resident_bytes;
        self.total_projection_loads += 1;
    }

    pub fn noteUnload(self: *SharedResidency, coord: ExpertCoord, projection_mask: u8, resident_bytes: usize) void {
        if (coord.layer_index >= self.layers.items.len) return;
        var layer = &self.layers.items[coord.layer_index];
        if (coord.expert_index >= layer.experts.len) return;
        var expert = &layer.experts[coord.expert_index];
        if ((expert.resident_projection_mask & projection_mask) == 0) return;
        expert.resident_projection_mask &= ~projection_mask;
        if (expert.resident_projection_count > 0) expert.resident_projection_count -= 1;
        expert.resident_bytes -|= resident_bytes;
        self.resident_bytes -|= resident_bytes;
        self.total_projection_evictions += 1;
    }

    pub fn isOverCapacity(self: *const SharedResidency, layer_index: usize) bool {
        if (self.max_resident_experts_per_layer == 0) return false;
        return self.residentExpertCount(layer_index) > self.max_resident_experts_per_layer;
    }

    pub fn residentExpertCount(self: *const SharedResidency, layer_index: usize) usize {
        if (layer_index >= self.layers.items.len) return 0;
        return self.layers.items[layer_index].residentExpertCount();
    }

    pub fn priority(self: *const SharedResidency, coord: ExpertCoord) ExpertPriority {
        if (coord.layer_index >= self.layers.items.len) return .{};
        const layer = &self.layers.items[coord.layer_index];
        if (coord.expert_index >= layer.experts.len) return .{};
        const expert = &layer.experts[coord.expert_index];
        return .{
            .touch_count = expert.touch_count,
            .last_touched_step = expert.last_touched_step,
        };
    }

    pub fn isMoreEvictable(self: *const SharedResidency, lhs: ExpertCoord, rhs: ExpertCoord) bool {
        const lhs_pri = self.priority(lhs);
        const rhs_pri = self.priority(rhs);
        if (lhs_pri.touch_count != rhs_pri.touch_count) return lhs_pri.touch_count < rhs_pri.touch_count;
        if (lhs_pri.last_touched_step != rhs_pri.last_touched_step) return lhs_pri.last_touched_step < rhs_pri.last_touched_step;
        return lhs.expert_index < rhs.expert_index;
    }

    fn ensureExpert(self: *SharedResidency, layer_index: usize, num_experts: usize) !void {
        while (self.layers.items.len <= layer_index) {
            try self.layers.append(self.allocator, .{});
        }
        try self.layers.items[layer_index].ensureExpertCount(self.allocator, num_experts);
    }
};

test "shared residency tracks hot and resident experts" {
    const allocator = std.testing.allocator;
    var residency = SharedResidency.init(allocator, 2);
    defer residency.deinit();

    const a = ExpertCoord{ .layer_index = 0, .expert_index = 0 };
    const b = ExpertCoord{ .layer_index = 0, .expert_index = 1 };
    const c = ExpertCoord{ .layer_index = 0, .expert_index = 2 };

    try residency.noteTouch(a, 4);
    try residency.noteTouch(a, 4);
    try residency.noteTouch(b, 4);
    try residency.noteLoad(a, 4, 0x1, 64);
    try residency.noteLoad(b, 4, 0x1, 64);
    try residency.noteLoad(c, 4, 0x1, 64);

    try std.testing.expect(residency.isOverCapacity(0));
    try std.testing.expect(residency.isMoreEvictable(c, a));
    try std.testing.expect(residency.isMoreEvictable(b, a));

    residency.noteUnload(c, 0x1, 64);
    try std.testing.expectEqual(@as(usize, 2), residency.residentExpertCount(0));
}
