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

// Phase H5: per-LoRA-block optimizer state manager that composes 8-bit
// AdamW (optimizers_ext.AdamW8BitState) with the NVMe spill tier and the
// training memory coordinator. Registers each param's grad block with the
// coordinator for unified residency tracking, then lets cold blocks evict
// their int8 moments + f32 scales to disk and rehydrate on the next step.

const std = @import("std");
const optimizers_ext = @import("optimizers_ext.zig");
const nvme_mod = @import("nvme_tier.zig");
const residency_mod = @import("grad_residency.zig");
const coord_mod = @import("training_memory_coordinator.zig");

pub const ParamId = residency_mod.GradBlockId;

pub const SpillSlots = struct {
    m_q_slot: nvme_mod.NvmeSlot,
    v_q_slot: nvme_mod.NvmeSlot,
    m_scale_slot: nvme_mod.NvmeSlot,
    v_scale_slot: nvme_mod.NvmeSlot,
};

pub const RegisteredParam = struct {
    id: ParamId,
    /// Pointer to the live (caller-owned) parameter weights. The manager
    /// calls `adamW8BitStep` against this slice on `step`. Must remain valid
    /// for the lifetime of the registration.
    weights: []f32,
    /// Optimizer state (int8 quantized m, v + per-block scales).
    state: optimizers_ext.AdamW8BitState,
    /// Config for this param's AdamW step (per-param LR overrides handled here).
    config: optimizers_ext.AdamW8BitConfig,
    /// Original logical length of the m_q/v_q arrays. Preserved so we can
    /// rehydrate after we free them on spill.
    size: usize,
    /// Number of quantization blocks (scale array length).
    n_blocks: usize,
    /// If non-null, this param's optimizer state has been spilled to NVMe.
    /// On the next `touchParam` or `step`, the manager reads it back.
    spilled_slots: ?SpillSlots = null,
};

pub const QuantizedOptimizerManagerConfig = struct {
    /// Default AdamW config used when registering a param without an override.
    default_config: optimizers_ext.AdamW8BitConfig = .{},
};

pub const QuantizedOptimizerManagerError = error{
    UnknownParam,
    NotSpillable,
    AlreadySpilled,
};

pub const QuantizedOptimizerManager = struct {
    allocator: std.mem.Allocator,
    /// Caller-owned coordinator (must outlive the manager).
    coord: *coord_mod.TrainingMemoryCoordinator,
    params: std.ArrayList(RegisteredParam),
    config: QuantizedOptimizerManagerConfig,

    /// Lifetime counters for observability.
    total_steps: u64 = 0,
    total_spills: u64 = 0,
    total_rehydrates: u64 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        coord: *coord_mod.TrainingMemoryCoordinator,
        config: QuantizedOptimizerManagerConfig,
    ) QuantizedOptimizerManager {
        return .{
            .allocator = allocator,
            .coord = coord,
            .params = .empty,
            .config = config,
        };
    }

    pub fn deinit(self: *QuantizedOptimizerManager) void {
        // Free any spilled NVMe slots, and drop in-RAM state for any param
        // that still owns it.
        const tier: ?*nvme_mod.NvmeTier = blk: {
            if (self.coord.owns_nvme) break :blk &self.coord.owned_nvme.?;
            break :blk self.coord.nvme;
        };
        for (self.params.items) |*p| {
            if (p.spilled_slots) |slots| {
                if (tier) |t| {
                    t.free(slots.m_q_slot);
                    t.free(slots.v_q_slot);
                    t.free(slots.m_scale_slot);
                    t.free(slots.v_scale_slot);
                }
                // m_q/v_q/m_scale/v_scale were freed during spill and now
                // point at static empty slices — do NOT call state.deinit().
            } else {
                p.state.deinit();
            }
        }
        self.params.deinit(self.allocator);
        self.* = undefined;
    }

    /// Compute how many bytes a given param's optimizer state takes in RAM.
    pub fn optimizerStateBytes(state: *const optimizers_ext.AdamW8BitState) u64 {
        const m_q_bytes: u64 = @intCast(state.m_q.len);
        const v_q_bytes: u64 = @intCast(state.v_q.len);
        const scale_bytes: u64 = @intCast((state.m_scale.len + state.v_scale.len) * @sizeOf(f32));
        return m_q_bytes + v_q_bytes + scale_bytes;
    }

    /// Register a parameter (e.g., a LoRA A or B block). Creates its
    /// AdamW8BitState and registers the corresponding grad block with the
    /// coordinator. `grad_bytes` is the weight size in bytes (for coordinator
    /// budget accounting of THIS param's grad; the optimizer state bytes are
    /// tracked separately via `optimizerStateBytes`).
    pub fn registerParam(
        self: *QuantizedOptimizerManager,
        id: ParamId,
        weights: []f32,
        grad_bytes: u64,
        override_config: ?optimizers_ext.AdamW8BitConfig,
    ) !void {
        const cfg = override_config orelse self.config.default_config;

        try self.coord.registerGradBlock(id, grad_bytes);
        errdefer {
            // Best-effort rollback: release the gradient budget reservation.
            // There is no public `unregister` on GradResidency in this
            // coordinator API, so the residency entry stays (in .cold state
            // with zero resident_bytes, so totalResident() is unaffected).
            self.coord.budget.release(.gradients, .host, grad_bytes);
        }

        var state = try optimizers_ext.AdamW8BitState.init(self.allocator, weights.len, cfg.block_size);
        errdefer state.deinit();

        const n_blocks = if (weights.len == 0) 0 else (weights.len + cfg.block_size - 1) / cfg.block_size;

        try self.params.append(self.allocator, .{
            .id = id,
            .weights = weights,
            .state = state,
            .config = cfg,
            .size = weights.len,
            .n_blocks = n_blocks,
            .spilled_slots = null,
        });
    }

    fn findIndex(self: *const QuantizedOptimizerManager, id: ParamId) ?usize {
        for (self.params.items, 0..) |*p, i| {
            if (p.id.eql(id)) return i;
        }
        return null;
    }

    fn nvmeTier(self: *QuantizedOptimizerManager) *nvme_mod.NvmeTier {
        if (self.coord.owns_nvme) return &self.coord.owned_nvme.?;
        return self.coord.nvme.?;
    }

    /// Run one AdamW8Bit step on the given param using the supplied gradient
    /// slice. If the param's optimizer state is currently spilled, rehydrate
    /// from NVMe first. Asserts `weights.len == grad.len`.
    pub fn step(
        self: *QuantizedOptimizerManager,
        id: ParamId,
        grad: []const f32,
        lr: f32,
    ) !void {
        const idx = self.findIndex(id) orelse return QuantizedOptimizerManagerError.UnknownParam;
        const p = &self.params.items[idx];
        std.debug.assert(p.weights.len == grad.len);

        if (p.spilled_slots != null) {
            try self.rehydrateState(idx);
        }

        optimizers_ext.adamW8BitStep(p.weights, grad, &p.state, lr, p.config);
        self.total_steps += 1;
    }

    /// Spill the optimizer state for a specific param to NVMe. Requires the
    /// param's grad block to be unpinned. Returns `error.NotSpillable` if the
    /// block is pinned.
    pub fn spillOptimizerState(
        self: *QuantizedOptimizerManager,
        id: ParamId,
    ) !void {
        const idx = self.findIndex(id) orelse return QuantizedOptimizerManagerError.UnknownParam;
        const p = &self.params.items[idx];
        if (p.spilled_slots != null) return QuantizedOptimizerManagerError.AlreadySpilled;

        if (self.coord.residency.entry(id)) |e| {
            if (e.pin_count > 0) return QuantizedOptimizerManagerError.NotSpillable;
        }

        const tier = self.nvmeTier();

        const m_q_bytes: u64 = @intCast(p.state.m_q.len);
        const v_q_bytes: u64 = @intCast(p.state.v_q.len);
        const m_scale_bytes: u64 = @intCast(p.state.m_scale.len * @sizeOf(f32));
        const v_scale_bytes: u64 = @intCast(p.state.v_scale.len * @sizeOf(f32));

        // Zero-length slots aren't supported by NvmeTier; if a param was
        // registered with size 0 we trivially mark it spilled with a fresh
        // state and return.
        if (m_q_bytes == 0 and v_q_bytes == 0 and m_scale_bytes == 0 and v_scale_bytes == 0) {
            self.total_spills += 1;
            return;
        }

        var m_q_slot: ?nvme_mod.NvmeSlot = null;
        var v_q_slot: ?nvme_mod.NvmeSlot = null;
        var m_scale_slot: ?nvme_mod.NvmeSlot = null;
        var v_scale_slot: ?nvme_mod.NvmeSlot = null;
        errdefer {
            if (m_q_slot) |s| tier.free(s);
            if (v_q_slot) |s| tier.free(s);
            if (m_scale_slot) |s| tier.free(s);
            if (v_scale_slot) |s| tier.free(s);
        }

        m_q_slot = try tier.allocate(m_q_bytes);
        v_q_slot = try tier.allocate(v_q_bytes);
        m_scale_slot = try tier.allocate(m_scale_bytes);
        v_scale_slot = try tier.allocate(v_scale_bytes);

        // i8 arrays reinterpreted as bytes.
        try tier.write(m_q_slot.?, std.mem.sliceAsBytes(p.state.m_q));
        try tier.write(v_q_slot.?, std.mem.sliceAsBytes(p.state.v_q));
        try tier.write(m_scale_slot.?, std.mem.sliceAsBytes(p.state.m_scale));
        try tier.write(v_scale_slot.?, std.mem.sliceAsBytes(p.state.v_scale));

        // Drop the in-RAM copies. This is the whole point of the spill —
        // optimizer state bytes are no longer counted in residentOptimizerBytes.
        self.allocator.free(p.state.m_q);
        self.allocator.free(p.state.v_q);
        self.allocator.free(p.state.m_scale);
        self.allocator.free(p.state.v_scale);
        p.state.m_q = &[_]i8{};
        p.state.v_q = &[_]i8{};
        p.state.m_scale = &[_]f32{};
        p.state.v_scale = &[_]f32{};

        p.spilled_slots = .{
            .m_q_slot = m_q_slot.?,
            .v_q_slot = v_q_slot.?,
            .m_scale_slot = m_scale_slot.?,
            .v_scale_slot = v_scale_slot.?,
        };
        self.total_spills += 1;
    }

    fn rehydrateState(self: *QuantizedOptimizerManager, idx: usize) !void {
        const p = &self.params.items[idx];
        const slots = p.spilled_slots orelse return;
        const tier = self.nvmeTier();

        const m_q = try self.allocator.alloc(i8, p.size);
        errdefer self.allocator.free(m_q);
        const v_q = try self.allocator.alloc(i8, p.size);
        errdefer self.allocator.free(v_q);
        const m_scale = try self.allocator.alloc(f32, p.n_blocks);
        errdefer self.allocator.free(m_scale);
        const v_scale = try self.allocator.alloc(f32, p.n_blocks);
        errdefer self.allocator.free(v_scale);

        try tier.read(slots.m_q_slot, std.mem.sliceAsBytes(m_q));
        try tier.read(slots.v_q_slot, std.mem.sliceAsBytes(v_q));
        try tier.read(slots.m_scale_slot, std.mem.sliceAsBytes(m_scale));
        try tier.read(slots.v_scale_slot, std.mem.sliceAsBytes(v_scale));

        tier.free(slots.m_q_slot);
        tier.free(slots.v_q_slot);
        tier.free(slots.m_scale_slot);
        tier.free(slots.v_scale_slot);

        p.state.m_q = m_q;
        p.state.v_q = v_q;
        p.state.m_scale = m_scale;
        p.state.v_scale = v_scale;
        p.spilled_slots = null;
        self.total_rehydrates += 1;
    }

    /// Between-microbatch cleanup: for every param whose grad block is cold
    /// (touch_count below `min_touch`), spill its optimizer state to NVMe.
    /// Returns the number of params spilled.
    pub fn spillColdOptimizerStates(
        self: *QuantizedOptimizerManager,
        min_touch: u32,
    ) !u32 {
        var count: u32 = 0;
        var i: usize = 0;
        while (i < self.params.items.len) : (i += 1) {
            const id = self.params.items[i].id;
            if (self.params.items[i].spilled_slots != null) continue;
            const e = self.coord.residency.entry(id) orelse continue;
            if (e.touch_count >= min_touch) continue;
            if (e.pin_count != 0) continue;
            try self.spillOptimizerState(id);
            count += 1;
        }
        return count;
    }

    /// Total bytes of currently-resident (non-spilled) optimizer state
    /// across all params. Useful for `TrainingBudget` budgeting.
    pub fn residentOptimizerBytes(self: *const QuantizedOptimizerManager) u64 {
        var total: u64 = 0;
        for (self.params.items) |*p| {
            if (p.spilled_slots != null) continue;
            total += optimizerStateBytes(&p.state);
        }
        return total;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

fn makeCoord(tmp: *std.testing.TmpDir) !coord_mod.TrainingMemoryCoordinator {
    return coord_mod.TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 4 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "opt_scratch.bin",
        .nvme_max_bytes = 1 * 1024 * 1024,
    });
}

test "init + deinit round-trips cleanly" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    try testing.expectEqual(@as(usize, 0), mgr.params.items.len);
    try testing.expectEqual(@as(u64, 0), mgr.total_steps);
}

test "registerParam creates AdamW8BitState and bumps resident bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    var weights = @as([64]f32, @splat(0));
    const id = ParamId{ .layer_idx = 0, .module_idx = 0 };
    try mgr.registerParam(id, &weights, @sizeOf(f32) * weights.len, null);

    try testing.expectEqual(@as(usize, 1), mgr.params.items.len);
    // int8 m + int8 v + f32 m_scale + f32 v_scale. With default block_size
    // 128 and size 64, n_blocks = 1, so 64 + 64 + 4 + 4 = 136.
    try testing.expectEqual(@as(u64, 136), mgr.residentOptimizerBytes());
    // Coordinator saw the grad block.
    try testing.expect(coord.residency.entry(id) != null);
}

test "step runs an AdamW8Bit update and increments total_steps" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    var weights = @as([32]f32, @splat(0.5));
    const id = ParamId{ .layer_idx = 0, .module_idx = 0 };
    try mgr.registerParam(id, &weights, @sizeOf(f32) * weights.len, optimizers_ext.AdamW8BitConfig{ .block_size = 32, .weight_decay = 0.0 });

    const grad = @as([32]f32, @splat(0.1));
    try mgr.step(id, &grad, 0.01);

    try testing.expectEqual(@as(u64, 1), mgr.total_steps);
    // Weights should have moved off their initial value.
    try testing.expect(weights[0] != 0.5);
}

test "step on an unknown id returns UnknownParam" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    const grad = @as([4]f32, @splat(0));
    const missing = ParamId{ .layer_idx = 99, .module_idx = 99 };
    try testing.expectError(
        QuantizedOptimizerManagerError.UnknownParam,
        mgr.step(missing, &grad, 0.01),
    );
}

test "spillOptimizerState writes to NVMe and drops resident bytes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    var weights = @as([64]f32, @splat(0));
    const id = ParamId{ .layer_idx = 1, .module_idx = 0 };
    try mgr.registerParam(id, &weights, @sizeOf(f32) * weights.len, optimizers_ext.AdamW8BitConfig{ .block_size = 32 });

    const before = mgr.residentOptimizerBytes();
    try testing.expect(before > 0);

    try mgr.spillOptimizerState(id);
    try testing.expect(mgr.params.items[0].spilled_slots != null);
    try testing.expectEqual(@as(u64, 0), mgr.residentOptimizerBytes());
    try testing.expectEqual(@as(u64, 1), mgr.total_spills);
}

test "step after spill rehydrates transparently; weights match unspilled ref" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    // Two params with identical init, same grad, same cfg.
    var wA = @as([32]f32, @splat(0.25));
    var wB = @as([32]f32, @splat(0.25));
    const idA = ParamId{ .layer_idx = 0, .module_idx = 0 };
    const idB = ParamId{ .layer_idx = 0, .module_idx = 1 };
    const cfg = optimizers_ext.AdamW8BitConfig{ .block_size = 32, .weight_decay = 0.0 };
    try mgr.registerParam(idA, &wA, @sizeOf(f32) * wA.len, cfg);
    try mgr.registerParam(idB, &wB, @sizeOf(f32) * wB.len, cfg);

    const grad = @as([32]f32, @splat(0.2));
    // First step on both so the quantized state has nonzero values worth
    // preserving through a spill round-trip.
    try mgr.step(idA, &grad, 0.01);
    try mgr.step(idB, &grad, 0.01);

    // Spill B, then take a second step on both.
    try mgr.spillOptimizerState(idB);
    try testing.expect(mgr.params.items[1].spilled_slots != null);

    try mgr.step(idA, &grad, 0.01);
    try mgr.step(idB, &grad, 0.01);

    // After step, B should have been rehydrated and counter incremented.
    try testing.expectEqual(@as(u64, 1), mgr.total_rehydrates);
    try testing.expect(mgr.params.items[1].spilled_slots == null);

    // Both params should now have identical weights (to within rounding).
    for (wA, wB) |a, b| {
        try testing.expectApproxEqAbs(a, b, 1e-5);
    }
}

test "spillOptimizerState on a pinned block returns NotSpillable" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    var weights = @as([32]f32, @splat(0));
    const id = ParamId{ .layer_idx = 2, .module_idx = 2 };
    try mgr.registerParam(id, &weights, @sizeOf(f32) * weights.len, optimizers_ext.AdamW8BitConfig{ .block_size = 32 });

    // Pin via the coordinator.
    try coord.touchGradBlock(id, &[_]u8{});
    try coord.pinGradBlock(id);

    try testing.expectError(
        QuantizedOptimizerManagerError.NotSpillable,
        mgr.spillOptimizerState(id),
    );
}

test "spillColdOptimizerStates spills only cold params" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    defer mgr.deinit();

    var w_hot = @as([32]f32, @splat(0));
    var w_cold = @as([32]f32, @splat(0));
    const hot = ParamId{ .layer_idx = 0, .module_idx = 0 };
    const cold = ParamId{ .layer_idx = 0, .module_idx = 1 };
    const cfg = optimizers_ext.AdamW8BitConfig{ .block_size = 32 };
    try mgr.registerParam(hot, &w_hot, @sizeOf(f32) * w_hot.len, cfg);
    try mgr.registerParam(cold, &w_cold, @sizeOf(f32) * w_cold.len, cfg);

    // Touch hot 6 times, cold once.
    var i: u32 = 0;
    while (i < 6) : (i += 1) try coord.touchGradBlock(hot, &[_]u8{});
    try coord.touchGradBlock(cold, &[_]u8{});

    const n = try mgr.spillColdOptimizerStates(5);
    try testing.expectEqual(@as(u32, 1), n);
    // Cold was spilled, hot was not.
    const hot_idx = mgr.findIndex(hot).?;
    const cold_idx = mgr.findIndex(cold).?;
    try testing.expect(mgr.params.items[hot_idx].spilled_slots == null);
    try testing.expect(mgr.params.items[cold_idx].spilled_slots != null);
}

test "deinit frees spilled slots and in-RAM state" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var coord = try makeCoord(&tmp);
    defer coord.deinit();

    var mgr = QuantizedOptimizerManager.init(testing.allocator, &coord, .{});
    // No explicit defer — we test deinit manually.

    var w0 = @as([32]f32, @splat(0));
    var w1 = @as([32]f32, @splat(0));
    const id0 = ParamId{ .layer_idx = 0, .module_idx = 0 };
    const id1 = ParamId{ .layer_idx = 0, .module_idx = 1 };
    const cfg = optimizers_ext.AdamW8BitConfig{ .block_size = 32 };
    try mgr.registerParam(id0, &w0, @sizeOf(f32) * w0.len, cfg);
    try mgr.registerParam(id1, &w1, @sizeOf(f32) * w1.len, cfg);

    // Spill one, leave the other resident.
    try mgr.spillOptimizerState(id0);
    mgr.deinit();
    // testing.allocator will catch leaks at end-of-test.
}
