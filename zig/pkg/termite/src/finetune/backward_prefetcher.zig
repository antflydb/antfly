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

// Backward-aware MoE expert prefetcher (Phase H4).
// Records expert activation per layer during the forward pass and replays the
// sequence in reverse so the backward pass can prefetch exactly the experts it
// is about to touch. Pairs with runtime/moe/* but does not depend on it.

const std = @import("std");

pub const ExpertCoord = struct {
    layer_idx: u32,
    expert_idx: u32,

    pub fn eql(a: ExpertCoord, b: ExpertCoord) bool {
        return a.layer_idx == b.layer_idx and a.expert_idx == b.expert_idx;
    }
};

pub const BackwardPrefetcherError = error{
    LayerOutOfRange,
};

pub const BackwardPrefetcherConfig = struct {
    /// Expected number of layers. Used to size internal arrays.
    num_layers: u32,
    /// Upper bound on active experts per layer per step. Used to size the
    /// per-layer ArrayList capacity hint.
    max_active_per_layer: u32 = 16,
};

pub const BackwardPrefetcher = struct {
    allocator: std.mem.Allocator,
    config: BackwardPrefetcherConfig,
    /// Per-layer list of expert IDs activated during forward. Index = layer_idx.
    /// Each sub-list may contain duplicates if the caller calls `noteForward`
    /// multiple times for the same layer in one step (e.g., MoE layers with
    /// multiple micro-batches). Duplicates are preserved — the prefetcher
    /// deduplicates at query time.
    per_layer: []std.ArrayList(u32),

    pub fn init(
        allocator: std.mem.Allocator,
        config: BackwardPrefetcherConfig,
    ) !BackwardPrefetcher {
        const lists = try allocator.alloc(std.ArrayList(u32), config.num_layers);
        for (lists) |*list| {
            list.* = .empty;
        }
        return .{
            .allocator = allocator,
            .config = config,
            .per_layer = lists,
        };
    }

    pub fn deinit(self: *BackwardPrefetcher) void {
        for (self.per_layer) |*list| {
            list.deinit(self.allocator);
        }
        self.allocator.free(self.per_layer);
        self.per_layer = &.{};
    }

    pub fn noteForward(
        self: *BackwardPrefetcher,
        layer_idx: u32,
        expert_ids: []const u32,
    ) !void {
        if (layer_idx >= self.config.num_layers) return BackwardPrefetcherError.LayerOutOfRange;
        var list = &self.per_layer[layer_idx];
        try list.ensureTotalCapacity(self.allocator, self.config.max_active_per_layer);
        for (expert_ids) |eid| {
            try list.append(self.allocator, eid);
        }
    }

    pub fn upcomingBackward(
        self: *const BackwardPrefetcher,
        allocator: std.mem.Allocator,
        current_layer: u32,
        lookahead_layers: u32,
    ) ![]ExpertCoord {
        var out: std.ArrayList(ExpertCoord) = .empty;
        errdefer out.deinit(allocator);

        if (lookahead_layers == 0 or current_layer == 0) {
            return try out.toOwnedSlice(allocator);
        }

        const start_exclusive = current_layer;
        const span = @min(lookahead_layers, current_layer);
        const end_inclusive = start_exclusive - span;

        var layer: u32 = start_exclusive;
        while (layer > end_inclusive) {
            layer -= 1;
            if (layer >= self.config.num_layers) continue;
            const items = self.per_layer[layer].items;
            for (items) |eid| {
                const coord: ExpertCoord = .{ .layer_idx = layer, .expert_idx = eid };
                var seen = false;
                for (out.items) |existing| {
                    if (existing.eql(coord)) {
                        seen = true;
                        break;
                    }
                }
                if (!seen) {
                    try out.append(allocator, coord);
                }
            }
        }

        return try out.toOwnedSlice(allocator);
    }

    pub fn reset(self: *BackwardPrefetcher) void {
        for (self.per_layer) |*list| {
            list.clearRetainingCapacity();
        }
    }

    pub fn totalRecorded(self: *const BackwardPrefetcher) usize {
        var total: usize = 0;
        for (self.per_layer) |list| {
            total += list.items.len;
        }
        return total;
    }

    pub fn distinctExpertsAtLayer(self: *const BackwardPrefetcher, layer_idx: u32) !usize {
        if (layer_idx >= self.config.num_layers) return BackwardPrefetcherError.LayerOutOfRange;
        const items = self.per_layer[layer_idx].items;
        var scratch: [256]u32 = undefined;
        var scratch_len: usize = 0;
        var overflow: std.ArrayList(u32) = .empty;
        defer overflow.deinit(self.allocator);

        for (items) |eid| {
            var seen = false;
            var i: usize = 0;
            while (i < scratch_len) : (i += 1) {
                if (scratch[i] == eid) {
                    seen = true;
                    break;
                }
            }
            if (!seen) {
                for (overflow.items) |v| {
                    if (v == eid) {
                        seen = true;
                        break;
                    }
                }
            }
            if (seen) continue;
            if (scratch_len < scratch.len) {
                scratch[scratch_len] = eid;
                scratch_len += 1;
            } else {
                try overflow.append(self.allocator, eid);
            }
        }
        return scratch_len + overflow.items.len;
    }
};

test "noteForward records to the right layer and totalRecorded counts" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 8 });
    defer pf.deinit();

    try pf.noteForward(0, &.{ 1, 2, 3 });
    try pf.noteForward(5, &.{ 7, 9 });
    try pf.noteForward(5, &.{4});

    try std.testing.expectEqual(@as(usize, 6), pf.totalRecorded());
    try std.testing.expectEqual(@as(usize, 3), pf.per_layer[0].items.len);
    try std.testing.expectEqual(@as(usize, 3), pf.per_layer[5].items.len);
    try std.testing.expectEqual(@as(usize, 0), pf.per_layer[1].items.len);
}

test "upcomingBackward returns experts from window below current_layer" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 8 });
    defer pf.deinit();

    try pf.noteForward(2, &.{10});
    try pf.noteForward(3, &.{ 20, 21 });
    try pf.noteForward(4, &.{30});
    try pf.noteForward(5, &.{99}); // should not appear

    const coords = try pf.upcomingBackward(allocator, 5, 2);
    defer allocator.free(coords);

    try std.testing.expectEqual(@as(usize, 3), coords.len);

    var saw_3_20 = false;
    var saw_3_21 = false;
    var saw_4_30 = false;
    for (coords) |c| {
        if (c.layer_idx == 3 and c.expert_idx == 20) saw_3_20 = true;
        if (c.layer_idx == 3 and c.expert_idx == 21) saw_3_21 = true;
        if (c.layer_idx == 4 and c.expert_idx == 30) saw_4_30 = true;
        try std.testing.expect(c.layer_idx != 5);
        try std.testing.expect(c.layer_idx != 2);
    }
    try std.testing.expect(saw_3_20 and saw_3_21 and saw_4_30);
}

test "upcomingBackward deduplicates repeated experts" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 8 });
    defer pf.deinit();

    try pf.noteForward(3, &.{ 1, 2, 1 });
    try pf.noteForward(3, &.{ 1, 2, 1 });

    const coords = try pf.upcomingBackward(allocator, 4, 1);
    defer allocator.free(coords);

    try std.testing.expectEqual(@as(usize, 2), coords.len);
    var saw_1 = false;
    var saw_2 = false;
    for (coords) |c| {
        try std.testing.expectEqual(@as(u32, 3), c.layer_idx);
        if (c.expert_idx == 1) saw_1 = true;
        if (c.expert_idx == 2) saw_2 = true;
    }
    try std.testing.expect(saw_1 and saw_2);
}

test "upcomingBackward with lookahead_layers=0 is empty" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 4 });
    defer pf.deinit();

    try pf.noteForward(0, &.{ 1, 2 });
    try pf.noteForward(1, &.{3});

    const coords = try pf.upcomingBackward(allocator, 3, 0);
    defer allocator.free(coords);
    try std.testing.expectEqual(@as(usize, 0), coords.len);
}

test "upcomingBackward with current_layer=0 is empty" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 4 });
    defer pf.deinit();

    try pf.noteForward(0, &.{ 1, 2 });

    const coords = try pf.upcomingBackward(allocator, 0, 5);
    defer allocator.free(coords);
    try std.testing.expectEqual(@as(usize, 0), coords.len);
}

test "upcomingBackward clamps lookahead at layer 0" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 8 });
    defer pf.deinit();

    try pf.noteForward(0, &.{100});
    try pf.noteForward(1, &.{200});
    try pf.noteForward(2, &.{300}); // should not appear

    const coords = try pf.upcomingBackward(allocator, 2, 10);
    defer allocator.free(coords);

    try std.testing.expectEqual(@as(usize, 2), coords.len);
    var saw_0 = false;
    var saw_1 = false;
    for (coords) |c| {
        if (c.layer_idx == 0 and c.expert_idx == 100) saw_0 = true;
        if (c.layer_idx == 1 and c.expert_idx == 200) saw_1 = true;
        try std.testing.expect(c.layer_idx != 2);
    }
    try std.testing.expect(saw_0 and saw_1);
}

test "reset clears all per-layer state" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 4 });
    defer pf.deinit();

    try pf.noteForward(0, &.{ 1, 2, 3 });
    try pf.noteForward(2, &.{ 4, 5 });
    try std.testing.expectEqual(@as(usize, 5), pf.totalRecorded());

    pf.reset();
    try std.testing.expectEqual(@as(usize, 0), pf.totalRecorded());

    const coords = try pf.upcomingBackward(allocator, 3, 3);
    defer allocator.free(coords);
    try std.testing.expectEqual(@as(usize, 0), coords.len);
}

test "noteForward rejects out-of-range layer_idx" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 4 });
    defer pf.deinit();

    try std.testing.expectError(
        BackwardPrefetcherError.LayerOutOfRange,
        pf.noteForward(4, &.{1}),
    );
    try std.testing.expectError(
        BackwardPrefetcherError.LayerOutOfRange,
        pf.noteForward(100, &.{1}),
    );
}

test "distinctExpertsAtLayer counts unique ids" {
    const allocator = std.testing.allocator;
    var pf = try BackwardPrefetcher.init(allocator, .{ .num_layers = 4 });
    defer pf.deinit();

    try pf.noteForward(1, &.{ 7, 7, 8, 9, 8 });
    try std.testing.expectEqual(@as(usize, 3), try pf.distinctExpertsAtLayer(1));
    try std.testing.expectEqual(@as(usize, 0), try pf.distinctExpertsAtLayer(0));
    try std.testing.expectError(
        BackwardPrefetcherError.LayerOutOfRange,
        pf.distinctExpertsAtLayer(4),
    );
}
