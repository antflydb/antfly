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

//! Bounded identity directory for a GPU-visible routed-expert slot arena.
//!
//! The directory owns no weight bytes. It is the authoritative mapping that
//! an arena owner uses while copying an expert into a fixed slot and while
//! publishing the matching expert-id -> slot table to a device. `observeRoute`
//! is atomic from the caller's perspective: it reports whether the route was
//! an all-hit *before* mutation, then installs every miss without evicting an
//! expert used by the same route. This distinction is required for a safe
//! optimistic device route: only `all_hit_before_update` may bypass miss
//! repair and its synchronization point.

const std = @import("std");

pub const max_layers: usize = 32;
pub const max_experts: usize = 128;
pub const max_slots: usize = 128;
pub const max_route_width: usize = 8;
pub const invalid_expert: u16 = std.math.maxInt(u16);
pub const invalid_slot: u8 = std.math.maxInt(u8);
const use_count_decay_interval: u64 = 1024;

pub const ArenaError = error{
    InvalidCapacity,
    LayerOutOfRange,
    ExpertOutOfRange,
    RouteTooWide,
    DuplicateRouteExpert,
};

pub const Observation = struct {
    route_slots: [max_route_width]u8 = @as([max_route_width]u8, @splat(invalid_slot)),
    /// True when this route entry was absent (or reverse-map-invalid) before
    /// observeRoute repaired the directory. Arena owners use this to upload
    /// exactly the slots whose weight bytes changed.
    repaired: [max_route_width]bool = @as([max_route_width]bool, @splat(false)),
    count: usize = 0,
    hit_count: usize = 0,
    miss_count: usize = 0,
    all_hit_before_update: bool = false,

    pub fn slots(self: *const Observation) []const u8 {
        return self.route_slots[0..self.count];
    }
};

pub const LayerSnapshot = struct {
    epoch: u64,
    expert_for_slot: [max_slots]u16,
    slot_for_expert: [max_experts]u8,
    use_count: [max_slots]u64,
    last_access_epoch: [max_slots]u64,
    layer_observation_count: u64,
};

/// Mutating operations are not thread-safe. An arena belongs to one compute
/// executor, and callers must serialize route observation, device publication,
/// and rollback as one transaction.
pub const ExpertSlotArena = struct {
    active_slots: u8,
    epoch: u64 = 0,
    expert_for_slot: [max_layers][max_slots]u16 =
        @as([max_layers][max_slots]u16, @splat(@as([max_slots]u16, @splat(invalid_expert)))),
    slot_for_expert: [max_layers][max_experts]u8 =
        @as([max_layers][max_experts]u8, @splat(@as([max_experts]u8, @splat(invalid_slot)))),
    use_count: [max_layers][max_slots]u64 =
        @as([max_layers][max_slots]u64, @splat(@as([max_slots]u64, @splat(0)))),
    last_access_epoch: [max_layers][max_slots]u64 =
        @as([max_layers][max_slots]u64, @splat(@as([max_slots]u64, @splat(0)))),
    layer_observation_count: [max_layers]u64 = @as([max_layers]u64, @splat(0)),

    pub fn init(active_slots: usize) ArenaError!ExpertSlotArena {
        if (active_slots == 0 or active_slots > max_slots) return error.InvalidCapacity;
        return .{ .active_slots = @intCast(active_slots) };
    }

    pub fn snapshotLayer(self: *const ExpertSlotArena, layer: usize) ArenaError!LayerSnapshot {
        if (layer >= max_layers) return error.LayerOutOfRange;
        return .{
            .epoch = self.epoch,
            .expert_for_slot = self.expert_for_slot[layer],
            .slot_for_expert = self.slot_for_expert[layer],
            .use_count = self.use_count[layer],
            .last_access_epoch = self.last_access_epoch[layer],
            .layer_observation_count = self.layer_observation_count[layer],
        };
    }

    pub fn restoreLayer(
        self: *ExpertSlotArena,
        layer: usize,
        snapshot: LayerSnapshot,
    ) ArenaError!void {
        if (layer >= max_layers) return error.LayerOutOfRange;
        self.epoch = snapshot.epoch;
        self.expert_for_slot[layer] = snapshot.expert_for_slot;
        self.slot_for_expert[layer] = snapshot.slot_for_expert;
        self.use_count[layer] = snapshot.use_count;
        self.last_access_epoch[layer] = snapshot.last_access_epoch;
        self.layer_observation_count[layer] = snapshot.layer_observation_count;
    }

    pub fn observeRoute(
        self: *ExpertSlotArena,
        layer: usize,
        expert_count: usize,
        experts: []const u32,
    ) ArenaError!Observation {
        if (layer >= max_layers) return error.LayerOutOfRange;
        if (expert_count == 0 or expert_count > max_experts) return error.ExpertOutOfRange;
        if (experts.len == 0 or experts.len > max_route_width or experts.len > self.active_slots) {
            return error.RouteTooWide;
        }
        for (experts, 0..) |expert, index| {
            if (expert >= expert_count) return error.ExpertOutOfRange;
            for (experts[0..index]) |prior| {
                if (prior == expert) return error.DuplicateRouteExpert;
            }
        }

        self.epoch +|= 1;
        self.layer_observation_count[layer] +|= 1;
        if (self.layer_observation_count[layer] % use_count_decay_interval == 0) {
            for (self.use_count[layer][0..self.active_slots]) |*count| count.* >>= 1;
        }
        var result = Observation{ .count = experts.len };
        var pinned = @as([max_slots]bool, @splat(false));

        // Observe first, before installing misses. This is the only state a
        // zero-sync device route is allowed to use as its all-hit proof.
        for (experts, 0..) |expert_u32, route_index| {
            const expert: usize = @intCast(expert_u32);
            const slot = self.slot_for_expert[layer][expert];
            if (slot == invalid_slot or slot >= self.active_slots) {
                result.miss_count += 1;
                continue;
            }
            const slot_index: usize = slot;
            if (self.expert_for_slot[layer][slot_index] != expert) {
                // A reverse-map mismatch fails closed as a miss and is
                // repaired below instead of exposing a stale arena offset.
                self.slot_for_expert[layer][expert] = invalid_slot;
                result.miss_count += 1;
                continue;
            }
            result.route_slots[route_index] = slot;
            result.hit_count += 1;
            pinned[slot_index] = true;
            self.noteUse(layer, slot_index);
        }
        result.all_hit_before_update = result.miss_count == 0;

        // Install misses without evicting any slot used by this route. Empty
        // slots win; otherwise use LFU with recency and slot index tie-breaks.
        for (experts, 0..) |expert_u32, route_index| {
            if (result.route_slots[route_index] != invalid_slot) continue;
            result.repaired[route_index] = true;
            const expert: usize = @intCast(expert_u32);
            const victim = self.chooseVictim(layer, pinned) orelse unreachable;
            const prior = self.expert_for_slot[layer][victim];
            if (prior != invalid_expert and prior < expert_count and
                self.slot_for_expert[layer][prior] == victim)
            {
                self.slot_for_expert[layer][prior] = invalid_slot;
            }
            self.expert_for_slot[layer][victim] = @intCast(expert);
            self.slot_for_expert[layer][expert] = @intCast(victim);
            self.use_count[layer][victim] = 0;
            self.noteUse(layer, victim);
            pinned[victim] = true;
            result.route_slots[route_index] = @intCast(victim);
        }
        return result;
    }

    pub fn writeExpertToSlotMap(
        self: *const ExpertSlotArena,
        layer: usize,
        expert_count: usize,
        output: []u32,
    ) ArenaError!void {
        if (layer >= max_layers) return error.LayerOutOfRange;
        if (expert_count == 0 or expert_count > max_experts or output.len < expert_count) {
            return error.ExpertOutOfRange;
        }
        for (output[0..expert_count], 0..) |*slot, expert| {
            const mapped = self.slot_for_expert[layer][expert];
            slot.* = if (mapped == invalid_slot or mapped >= self.active_slots)
                std.math.maxInt(u32)
            else
                mapped;
        }
    }

    /// A layer becomes eligible for optimistic device routing only after every
    /// physical slot has authoritative weight bytes.  The first synchronized
    /// route fills exactly `top_k` slots for qualified A4B (`top_k ==
    /// active_slots`); checking the reverse directory keeps this predicate
    /// independent of that model-specific equality and fails closed if a
    /// directory entry is inconsistent.
    pub fn layerIsFullyPopulated(self: *const ExpertSlotArena, layer: usize) bool {
        if (layer >= max_layers) return false;
        for (0..self.active_slots) |slot| {
            const expert = self.expert_for_slot[layer][slot];
            if (expert == invalid_expert or expert >= max_experts) return false;
            if (self.slot_for_expert[layer][expert] != slot) return false;
        }
        return true;
    }

    pub fn layersAreFullyPopulated(self: *const ExpertSlotArena, layer_count: usize) bool {
        if (layer_count == 0 or layer_count > max_layers) return false;
        for (0..layer_count) |layer| {
            if (!self.layerIsFullyPopulated(layer)) return false;
        }
        return true;
    }

    fn noteUse(self: *ExpertSlotArena, layer: usize, slot: usize) void {
        self.use_count[layer][slot] +|= 1;
        self.last_access_epoch[layer][slot] = self.epoch;
    }

    fn chooseVictim(self: *const ExpertSlotArena, layer: usize, pinned: [max_slots]bool) ?usize {
        var best: ?usize = null;
        for (0..self.active_slots) |slot| {
            if (pinned[slot]) continue;
            if (self.expert_for_slot[layer][slot] == invalid_expert) return slot;
            if (best) |current| {
                const use = self.use_count[layer][slot];
                const current_use = self.use_count[layer][current];
                const age = self.last_access_epoch[layer][slot];
                const current_age = self.last_access_epoch[layer][current];
                if (use < current_use or
                    (use == current_use and age < current_age) or
                    (use == current_use and age == current_age and slot < current))
                {
                    best = slot;
                }
            } else {
                best = slot;
            }
        }
        return best;
    }
};

test "expert slot arena distinguishes cold repair from proven all-hit" {
    var arena = try ExpertSlotArena.init(8);
    const route = [_]u32{ 7, 3, 12, 1, 9, 4, 2, 6 };
    const cold = try arena.observeRoute(0, 128, &route);
    try std.testing.expectEqual(@as(usize, 0), cold.hit_count);
    try std.testing.expectEqual(@as(usize, 8), cold.miss_count);
    try std.testing.expect(!cold.all_hit_before_update);
    for (cold.repaired[0..cold.count]) |repaired| try std.testing.expect(repaired);

    const warm = try arena.observeRoute(0, 128, &route);
    try std.testing.expectEqual(@as(usize, 8), warm.hit_count);
    try std.testing.expectEqual(@as(usize, 0), warm.miss_count);
    try std.testing.expect(warm.all_hit_before_update);
    for (warm.repaired[0..warm.count]) |repaired| try std.testing.expect(!repaired);
    try std.testing.expectEqualSlices(u8, cold.slots(), warm.slots());
}

test "expert slot arena repairs one miss without evicting current hits" {
    var arena = try ExpertSlotArena.init(8);
    const first = [_]u32{ 0, 1, 2, 3, 4, 5, 6, 7 };
    _ = try arena.observeRoute(4, 128, &first);
    const changed = [_]u32{ 0, 1, 2, 3, 4, 5, 6, 99 };
    const repaired = try arena.observeRoute(4, 128, &changed);
    try std.testing.expectEqual(@as(usize, 7), repaired.hit_count);
    try std.testing.expectEqual(@as(usize, 1), repaired.miss_count);
    try std.testing.expect(!repaired.all_hit_before_update);

    var map = @as([max_experts]u32, @splat(std.math.maxInt(u32)));
    try arena.writeExpertToSlotMap(4, 128, &map);
    for (changed) |expert| try std.testing.expect(map[expert] < 8);
    try std.testing.expectEqual(std.math.maxInt(u32), map[7]);
}

test "expert slot arena restores one transactional layer without a full arena copy" {
    try std.testing.expect(@sizeOf(LayerSnapshot) * 16 < @sizeOf(ExpertSlotArena));
    var arena = try ExpertSlotArena.init(8);
    const original = [_]u32{ 0, 1, 2, 3, 4, 5, 6, 7 };
    _ = try arena.observeRoute(4, 128, &original);
    const snapshot = try arena.snapshotLayer(4);
    _ = try arena.observeRoute(4, 128, &.{ 0, 1, 2, 3, 4, 5, 6, 99 });
    try arena.restoreLayer(4, snapshot);

    const restored = try arena.observeRoute(4, 128, &original);
    try std.testing.expect(restored.all_hit_before_update);
    try std.testing.expectEqual(@as(usize, 0), restored.miss_count);
}

test "expert slot arena periodically decays historical LFU bias" {
    var arena = try ExpertSlotArena.init(8);
    const route = [_]u32{ 0, 1, 2, 3, 4, 5, 6, 7 };
    _ = try arena.observeRoute(0, 128, &route);
    arena.use_count[0][0] = 64;
    arena.layer_observation_count[0] = use_count_decay_interval - 1;

    _ = try arena.observeRoute(0, 128, &route);
    try std.testing.expectEqual(@as(u64, 33), arena.use_count[0][0]);
}

test "expert slot arena isolates layers and rejects duplicate routes" {
    var arena = try ExpertSlotArena.init(8);
    const route = [_]u32{ 0, 1 };
    _ = try arena.observeRoute(0, 128, &route);
    const other_layer = try arena.observeRoute(1, 128, &route);
    try std.testing.expectEqual(@as(usize, 2), other_layer.miss_count);
    try std.testing.expectError(
        error.DuplicateRouteExpert,
        arena.observeRoute(0, 128, &.{ 7, 7 }),
    );
}

test "expert slot arena only checkpoints after every requested layer is full" {
    var arena = try ExpertSlotArena.init(2);
    try std.testing.expect(!arena.layersAreFullyPopulated(2));
    _ = try arena.observeRoute(0, 128, &.{ 3, 7 });
    try std.testing.expect(arena.layerIsFullyPopulated(0));
    try std.testing.expect(!arena.layersAreFullyPopulated(2));
    _ = try arena.observeRoute(1, 128, &.{ 11, 13 });
    try std.testing.expect(arena.layersAreFullyPopulated(2));
    try std.testing.expect(!arena.layersAreFullyPopulated(0));
    try std.testing.expect(!arena.layersAreFullyPopulated(max_layers + 1));
}
