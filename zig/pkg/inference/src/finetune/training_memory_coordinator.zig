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

// Integration layer that unifies the training budget, gradient residency
// tracker, gradient checkpointing harness, and NVMe spill tier behind a
// single entry point a trainer calls per step. Reserves budget up front,
// triggers adaptive NVMe spill on denial, and rehydrates spilled grad
// blocks on demand for the backward pass.

const std = @import("std");
const budget_mod = @import("training_budget.zig");
const residency_mod = @import("grad_residency.zig");
const checkpoint_mod = @import("gradient_checkpoint.zig");
const nvme_mod = @import("nvme_tier.zig");

pub const CoordinatorConfig = struct {
    budget_limits: budget_mod.TrainingBudgetLimits,
    /// If set, the coordinator owns an NvmeTier created from this path.
    /// Otherwise it uses an externally-provided NvmeTier (see `initExternal`).
    nvme_path: ?[]const u8 = null,
    nvme_max_bytes: u64 = 8 * 1024 * 1024 * 1024,
    /// Gradient checkpoint policy. `every_n_layers = 0` disables.
    checkpoint_policy: checkpoint_mod.CheckpointPolicy = .{},
    recompute_ctx: ?*anyopaque = null,
    recompute_fn: ?checkpoint_mod.RecomputeFn = null,
};

/// Spill record: gradient block id plus its NVMe slot.
pub const SpillRecord = struct {
    id: residency_mod.GradBlockId,
    slot: nvme_mod.NvmeSlot,
    /// Bytes of the spilled payload (matches slot.length).
    bytes: u64,
};

pub const CoordinatorStats = struct {
    spills_total: u64,
    rehydrates_total: u64,
    denied_reservations: u64,
    peak_host_bytes: u64,
    peak_backend_bytes: u64,
};

pub const CoordinatorError = error{
    NvmePathRequired,
    BudgetDenied,
    BlockNotResident,
    BlockAlreadySpilled,
    RehydrateBufferTooSmall,
};

pub const TrainingMemoryCoordinator = struct {
    allocator: std.mem.Allocator,
    budget: budget_mod.TrainingBudget,
    residency: residency_mod.GradResidency,
    harness: ?checkpoint_mod.CheckpointHarness,
    nvme: ?*nvme_mod.NvmeTier,
    /// Backing storage when the coordinator owns the NvmeTier.
    owned_nvme: ?nvme_mod.NvmeTier,
    owns_nvme: bool,
    /// Spill records keyed by block id — linear search is fine, block count
    /// is bounded by num_layers * num_modules (~ a few hundred max).
    spilled: std.ArrayList(SpillRecord),
    stats: CoordinatorStats,

    pub const PayloadFn = *const fn (ctx: *anyopaque, id: residency_mod.GradBlockId) ?[]const u8;

    /// Initialize with coordinator-owned NvmeTier (created from config.nvme_path,
    /// resolved under `dir`). Production callers pass `std.Io.Dir.cwd()`.
    pub fn init(
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        io: std.Io,
        config: CoordinatorConfig,
    ) !TrainingMemoryCoordinator {
        const path = config.nvme_path orelse return CoordinatorError.NvmePathRequired;

        var tier = try nvme_mod.NvmeTier.init(allocator, dir, io, .{
            .path = path,
            .max_bytes = config.nvme_max_bytes,
        });
        errdefer tier.deinit();

        var self = TrainingMemoryCoordinator{
            .allocator = allocator,
            .budget = budget_mod.TrainingBudget.init(config.budget_limits),
            .residency = residency_mod.GradResidency.init(allocator),
            .harness = null,
            .nvme = null,
            .owned_nvme = tier,
            .owns_nvme = true,
            .spilled = .empty,
            .stats = .{
                .spills_total = 0,
                .rehydrates_total = 0,
                .denied_reservations = 0,
                .peak_host_bytes = 0,
                .peak_backend_bytes = 0,
            },
        };
        if (config.recompute_fn) |rfn| {
            const ctx = config.recompute_ctx orelse @as(*anyopaque, @ptrFromInt(@alignOf(usize)));
            self.harness = checkpoint_mod.CheckpointHarness.init(allocator, ctx, rfn);
        }
        return self;
    }

    /// Alternative: use a caller-owned NvmeTier (coordinator does not take ownership).
    pub fn initExternal(
        allocator: std.mem.Allocator,
        config: CoordinatorConfig,
        nvme: *nvme_mod.NvmeTier,
    ) !TrainingMemoryCoordinator {
        var self = TrainingMemoryCoordinator{
            .allocator = allocator,
            .budget = budget_mod.TrainingBudget.init(config.budget_limits),
            .residency = residency_mod.GradResidency.init(allocator),
            .harness = null,
            .nvme = nvme,
            .owned_nvme = null,
            .owns_nvme = false,
            .spilled = .empty,
            .stats = .{
                .spills_total = 0,
                .rehydrates_total = 0,
                .denied_reservations = 0,
                .peak_host_bytes = 0,
                .peak_backend_bytes = 0,
            },
        };
        if (config.recompute_fn) |rfn| {
            const ctx = config.recompute_ctx orelse @as(*anyopaque, @ptrFromInt(@alignOf(usize)));
            self.harness = checkpoint_mod.CheckpointHarness.init(allocator, ctx, rfn);
        }
        return self;
    }

    pub fn deinit(self: *TrainingMemoryCoordinator) void {
        self.spilled.deinit(self.allocator);
        if (self.harness) |*h| h.deinit();
        self.residency.deinit();
        if (self.owns_nvme) {
            if (self.owned_nvme) |*t| t.deinit();
        }
        self.* = undefined;
    }

    fn nvmeTier(self: *TrainingMemoryCoordinator) *nvme_mod.NvmeTier {
        if (self.owns_nvme) return &self.owned_nvme.?;
        return self.nvme.?;
    }

    fn findSpilledIndex(self: *const TrainingMemoryCoordinator, id: residency_mod.GradBlockId) ?usize {
        for (self.spilled.items, 0..) |rec, i| {
            if (rec.id.eql(id)) return i;
        }
        return null;
    }

    fn updatePeak(self: *TrainingMemoryCoordinator) void {
        var host_total: u64 = 0;
        var backend_total: u64 = 0;
        for (self.budget.host_by_category) |v| host_total += v;
        for (self.budget.backend_by_category) |v| backend_total += v;
        if (host_total > self.stats.peak_host_bytes) self.stats.peak_host_bytes = host_total;
        if (backend_total > self.stats.peak_backend_bytes) self.stats.peak_backend_bytes = backend_total;
    }

    // ─── Budget + residency ────────────────────────────────────────────

    pub fn registerGradBlock(
        self: *TrainingMemoryCoordinator,
        id: residency_mod.GradBlockId,
        bytes: u64,
    ) !void {
        const result = self.budget.tryReserve(.gradients, .host, bytes);
        if (result.event != .admitted) {
            self.stats.denied_reservations += 1;
            return CoordinatorError.BudgetDenied;
        }
        self.residency.register(id, bytes) catch |err| {
            self.budget.release(.gradients, .host, bytes);
            return err;
        };
        self.updatePeak();
    }

    pub fn touchGradBlock(
        self: *TrainingMemoryCoordinator,
        id: residency_mod.GradBlockId,
        rehydrate_dest: []u8,
    ) !void {
        if (self.findSpilledIndex(id)) |idx| {
            const rec = self.spilled.items[idx];
            if (rehydrate_dest.len < rec.bytes) return CoordinatorError.RehydrateBufferTooSmall;
            const tier = self.nvmeTier();
            try tier.read(rec.slot, rehydrate_dest[0..rec.bytes]);
            tier.free(rec.slot);
            _ = self.spilled.swapRemove(idx);
            // Re-reserve host gradients budget for the rehydrated block.
            const result = self.budget.tryReserve(.gradients, .host, rec.bytes);
            if (result.event != .admitted) {
                self.stats.denied_reservations += 1;
                return CoordinatorError.BudgetDenied;
            }
            self.stats.rehydrates_total += 1;
        }
        try self.residency.touch(id);
        self.updatePeak();
    }

    /// Pin alias for pinGradBlock use.
    pub fn pinGradBlock(self: *TrainingMemoryCoordinator, id: residency_mod.GradBlockId) !void {
        try self.residency.pin(id);
    }

    pub fn unpinGradBlock(self: *TrainingMemoryCoordinator, id: residency_mod.GradBlockId) !void {
        try self.residency.unpin(id);
    }

    // ─── Spill / rehydrate ─────────────────────────────────────────────

    pub fn spillGradBlock(
        self: *TrainingMemoryCoordinator,
        id: residency_mod.GradBlockId,
        payload: []const u8,
    ) !void {
        const e = self.residency.entry(id) orelse return residency_mod.GradResidencyError.UnknownBlock;
        if (e.pin_count != 0) return residency_mod.GradResidencyError.BlockIsPinned;
        if (e.state != .resident) return CoordinatorError.BlockNotResident;
        if (self.findSpilledIndex(id) != null) return CoordinatorError.BlockAlreadySpilled;

        const bytes: u64 = @intCast(payload.len);
        const tier = self.nvmeTier();
        const slot = try tier.allocate(bytes);
        errdefer tier.free(slot);
        try tier.write(slot, payload);

        try self.spilled.append(self.allocator, .{
            .id = id,
            .slot = slot,
            .bytes = bytes,
        });
        errdefer _ = self.spilled.pop();

        try self.residency.markSpilled(id);
        self.budget.release(.gradients, .host, bytes);
        self.stats.spills_total += 1;
    }

    pub fn spillToFit(
        self: *TrainingMemoryCoordinator,
        upcoming_peak: []const budget_mod.TrainingBudget.PeakEstimate,
        ctx: *anyopaque,
        payload_fn: PayloadFn,
    ) !u32 {
        var spilled_count: u32 = 0;
        while (true) {
            const report = self.budget.estimatePeak(upcoming_peak);
            if (report.fits_host and report.fits_backend) return spilled_count;

            const victim = self.residency.pickEvictable() orelse return spilled_count;
            const payload = payload_fn(ctx, victim) orelse {
                // No payload available — to avoid looping forever on the same
                // victim, bail. Caller can retry with a richer lookup.
                return spilled_count;
            };
            try self.spillGradBlock(victim, payload);
            spilled_count += 1;
        }
    }

    // ─── Checkpoint harness passthrough ────────────────────────────────

    pub fn recordSegment(
        self: *TrainingMemoryCoordinator,
        start_layer: u32,
        end_layer: u32,
        input: []const f32,
        total_tokens: usize,
        hidden_size: usize,
        user_data: ?*anyopaque,
    ) !u32 {
        if (self.harness) |*h| {
            return h.recordSegment(start_layer, end_layer, input, total_tokens, hidden_size, user_data);
        }
        return error.CheckpointHarnessDisabled;
    }

    pub fn recomputeSegment(
        self: *TrainingMemoryCoordinator,
        segment_idx: u32,
        out_hidden: []f32,
    ) !void {
        if (self.harness) |*h| {
            return h.recomputeSegment(segment_idx, out_hidden);
        }
        return error.CheckpointHarnessDisabled;
    }

    pub fn resetSegments(self: *TrainingMemoryCoordinator) void {
        if (self.harness) |*h| h.reset();
    }

    // ─── Stats ─────────────────────────────────────────────────────────

    pub fn statsSnapshot(self: *const TrainingMemoryCoordinator) CoordinatorStats {
        return self.stats;
    }

    pub fn format(self: *const TrainingMemoryCoordinator, out: []u8) ![]u8 {
        return std.fmt.bufPrint(
            out,
            "coord: spills={d} rehydrates={d} denied={d} peak_host={d} peak_backend={d} spilled_blocks={d}",
            .{
                self.stats.spills_total,
                self.stats.rehydrates_total,
                self.stats.denied_reservations,
                self.stats.peak_host_bytes,
                self.stats.peak_backend_bytes,
                self.spilled.items.len,
            },
        );
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "init with nvme_path succeeds and deinit cleans up" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 16 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
        .nvme_max_bytes = 4 * 1024 * 1024,
    });
    defer coord.deinit();

    // The file must exist.
    const stat = try tmp.dir.statFile(testing.io, "scratch.bin", .{});
    try testing.expect(stat.kind == .file);
}

test "init without nvme_path errors" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const result = TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1024 },
    });
    try testing.expectError(CoordinatorError.NvmePathRequired, result);
}

test "registerGradBlock reserves budget and residency entry" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    const id = residency_mod.GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    try coord.registerGradBlock(id, 4096);

    try testing.expect(coord.residency.entry(id) != null);
    try testing.expectEqual(
        @as(u64, 4096),
        coord.budget.host_by_category[@intFromEnum(budget_mod.BudgetCategory.gradients)],
    );
}

test "registerGradBlock returns BudgetDenied when full" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    const id = residency_mod.GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    const result = coord.registerGradBlock(id, 4096);
    try testing.expectError(CoordinatorError.BudgetDenied, result);
    try testing.expectEqual(@as(u64, 1), coord.stats.denied_reservations);
    try testing.expect(coord.residency.entry(id) == null);
}

test "spillGradBlock round-trips via NVMe and touchGradBlock rehydrates" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    const id = residency_mod.GradBlockId{ .layer_idx = 2, .module_idx = 1 };
    try coord.registerGradBlock(id, 16);
    try coord.touchGradBlock(id, &[_]u8{});

    const payload = [_]u8{ 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19 };
    try coord.spillGradBlock(id, &payload);

    try testing.expectEqual(residency_mod.GradBlockState.spilled, coord.residency.entry(id).?.state);
    try testing.expectEqual(@as(usize, 1), coord.spilled.items.len);
    try testing.expectEqual(@as(u64, 1), coord.stats.spills_total);
    // Budget released after spill.
    try testing.expectEqual(
        @as(u64, 0),
        coord.budget.host_by_category[@intFromEnum(budget_mod.BudgetCategory.gradients)],
    );

    var dest: [16]u8 = undefined;
    try coord.touchGradBlock(id, &dest);
    try testing.expectEqualSlices(u8, &payload, &dest);
    try testing.expectEqual(@as(u64, 1), coord.stats.rehydrates_total);
    try testing.expectEqual(residency_mod.GradBlockState.resident, coord.residency.entry(id).?.state);
    try testing.expectEqual(@as(usize, 0), coord.spilled.items.len);
    // Budget re-reserved after rehydrate.
    try testing.expectEqual(
        @as(u64, 16),
        coord.budget.host_by_category[@intFromEnum(budget_mod.BudgetCategory.gradients)],
    );
}

test "spillGradBlock fails if pinned" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    const id = residency_mod.GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    try coord.registerGradBlock(id, 8);
    try coord.touchGradBlock(id, &[_]u8{});
    try coord.pinGradBlock(id);

    const payload = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try testing.expectError(
        residency_mod.GradResidencyError.BlockIsPinned,
        coord.spillGradBlock(id, &payload),
    );
}

test "pin/unpin ref-counts through to residency" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    const id = residency_mod.GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    try coord.registerGradBlock(id, 8);
    try coord.touchGradBlock(id, &[_]u8{});
    try coord.pinGradBlock(id);
    try coord.pinGradBlock(id);
    try testing.expectEqual(@as(u32, 2), coord.residency.entry(id).?.pin_count);
    try coord.unpinGradBlock(id);
    try coord.unpinGradBlock(id);
    try testing.expectEqual(@as(u32, 0), coord.residency.entry(id).?.pin_count);
    try testing.expectError(
        residency_mod.GradResidencyError.NotPinned,
        coord.unpinGradBlock(id),
    );
}

const TestPayloadCtx = struct {
    ids: []const residency_mod.GradBlockId,
    payloads: []const []const u8,

    fn lookup(ctx: *anyopaque, id: residency_mod.GradBlockId) ?[]const u8 {
        const self: *TestPayloadCtx = @ptrCast(@alignCast(ctx));
        for (self.ids, 0..) |bid, i| {
            if (bid.eql(id)) return self.payloads[i];
        }
        return null;
    }
};

test "spillToFit spills least-valuable blocks until upcoming peak fits" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 2048, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    const a = residency_mod.GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    const b = residency_mod.GradBlockId{ .layer_idx = 0, .module_idx = 1 };
    try coord.registerGradBlock(a, 512);
    try coord.registerGradBlock(b, 512);
    // `a` is hot (touched many times); `b` is cold-ish (touched once).
    try coord.touchGradBlock(a, &[_]u8{});
    try coord.touchGradBlock(a, &[_]u8{});
    try coord.touchGradBlock(a, &[_]u8{});
    try coord.touchGradBlock(b, &[_]u8{});

    var pa: [512]u8 = undefined;
    var pb: [512]u8 = undefined;
    @memset(&pa, 0xAA);
    @memset(&pb, 0xBB);
    const ids = [_]residency_mod.GradBlockId{ a, b };
    const payloads = [_][]const u8{ &pa, &pb };
    var ctx = TestPayloadCtx{ .ids = &ids, .payloads = &payloads };

    // Upcoming peak 1200 bytes on host, plus 1024 already in gradients = 2224 > 2048.
    // Need to spill one 512-byte block.
    const peak = [_]budget_mod.TrainingBudget.PeakEstimate{
        .{ .category = .activations, .tier = .host, .bytes = 1200 },
    };
    const n = try coord.spillToFit(&peak, @ptrCast(&ctx), TestPayloadCtx.lookup);
    try testing.expectEqual(@as(u32, 1), n);
    // The cold-ish `b` should have been chosen.
    try testing.expectEqual(residency_mod.GradBlockState.spilled, coord.residency.entry(b).?.state);
    try testing.expectEqual(residency_mod.GradBlockState.resident, coord.residency.entry(a).?.state);
}

const RecomputeCtx = struct {
    call_count: u32 = 0,
};

fn testRecompute(
    ctx: *anyopaque,
    segment: checkpoint_mod.SegmentToken,
    out_hidden: []f32,
) anyerror!void {
    const m: *RecomputeCtx = @ptrCast(@alignCast(ctx));
    m.call_count += 1;
    const n = @min(segment.input.len, out_hidden.len);
    @memcpy(out_hidden[0..n], segment.input[0..n]);
}

test "checkpoint harness passthrough: record/recompute/reset" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var rctx = RecomputeCtx{};
    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
        .recompute_ctx = @ptrCast(&rctx),
        .recompute_fn = testRecompute,
    });
    defer coord.deinit();

    const in0 = [_]f32{ 1, 2, 3, 4 };
    const idx = try coord.recordSegment(0, 4, &in0, 1, 4, null);
    try testing.expectEqual(@as(u32, 0), idx);

    var out: [4]f32 = undefined;
    try coord.recomputeSegment(idx, &out);
    try testing.expectEqual(@as(u32, 1), rctx.call_count);
    try testing.expectEqual(@as(f32, 1), out[0]);
    try testing.expectEqual(@as(f32, 4), out[3]);

    coord.resetSegments();
    try testing.expectError(
        error.InvalidSegmentIndex,
        coord.recomputeSegment(0, &out),
    );
}

test "stats track spills and rehydrates" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1 * 1024 * 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    const id = residency_mod.GradBlockId{ .layer_idx = 1, .module_idx = 2 };
    try coord.registerGradBlock(id, 8);
    try coord.touchGradBlock(id, &[_]u8{});

    const payload = [_]u8{ 9, 8, 7, 6, 5, 4, 3, 2 };
    try coord.spillGradBlock(id, &payload);
    try testing.expectEqual(@as(u64, 1), coord.statsSnapshot().spills_total);
    try testing.expectEqual(@as(u64, 0), coord.statsSnapshot().rehydrates_total);

    var dest: [8]u8 = undefined;
    try coord.touchGradBlock(id, &dest);
    const snap = coord.statsSnapshot();
    try testing.expectEqual(@as(u64, 1), snap.spills_total);
    try testing.expectEqual(@as(u64, 1), snap.rehydrates_total);
    try testing.expect(snap.peak_host_bytes >= 8);
}

test "initExternal uses caller-owned NvmeTier and does not close it" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var tier = try nvme_mod.NvmeTier.init(testing.allocator, tmp.dir, testing.io, .{
        .path = "external.bin",
        .max_bytes = 1 * 1024 * 1024,
    });
    defer tier.deinit(); // caller owns it; still valid after coordinator deinit.

    {
        var coord = try TrainingMemoryCoordinator.initExternal(
            testing.allocator,
            .{
                .budget_limits = .{ .host_bytes = 1 * 1024 * 1024, .scratch_headroom_bytes = 0 },
            },
            &tier,
        );
        defer coord.deinit();

        try testing.expect(!coord.owns_nvme);

        const id = residency_mod.GradBlockId{ .layer_idx = 0, .module_idx = 0 };
        try coord.registerGradBlock(id, 4);
        try coord.touchGradBlock(id, &[_]u8{});
        const payload = [_]u8{ 1, 2, 3, 4 };
        try coord.spillGradBlock(id, &payload);
    }

    // Tier is still usable after the coordinator was deinit'd.
    const slot = try tier.allocate(4);
    const probe = [_]u8{ 7, 7, 7, 7 };
    try tier.write(slot, &probe);
    var out: [4]u8 = undefined;
    try tier.read(slot, &out);
    try testing.expectEqualSlices(u8, &probe, &out);
}

test "format writes a human readable summary" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var coord = try TrainingMemoryCoordinator.init(testing.allocator, tmp.dir, testing.io, .{
        .budget_limits = .{ .host_bytes = 1024, .scratch_headroom_bytes = 0 },
        .nvme_path = "scratch.bin",
    });
    defer coord.deinit();

    var buf: [256]u8 = undefined;
    const s = try coord.format(&buf);
    try testing.expect(std.mem.indexOf(u8, s, "spills=0") != null);
    try testing.expect(std.mem.indexOf(u8, s, "rehydrates=0") != null);
}
