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

// Three-way training memory budget (weights / gradients / activations / optimizer).
// Parallel to runtime/tier/memory.zig's RunBudget but training-specific and
// standalone: lets a trainer reason about peak host+backend usage and refuse
// reservations that would OOM before the allocator actually fails.

const std = @import("std");

pub const TrainingBudgetLimits = struct {
    host_bytes: u64 = std.math.maxInt(u64),
    backend_bytes: u64 = std.math.maxInt(u64),
    optimizer_reserve_bytes: u64 = 0,
    scratch_headroom_bytes: u64 = 64 * 1024 * 1024,
};

pub const BudgetCategory = enum {
    weights,
    gradients,
    activations,
    optimizer,
};

pub const BudgetEvent = enum {
    admitted,
    denied_host_oom,
    denied_backend_oom,
    denied_optimizer_overlap,
};

pub const ReservationResult = struct {
    event: BudgetEvent,
    reserved: u64,
    host_remaining: u64,
    backend_remaining: u64,
};

pub const TrainingBudget = struct {
    pub const Tier = enum { host, backend };

    pub const PeakEstimate = struct {
        category: BudgetCategory,
        tier: Tier,
        bytes: u64,
    };

    pub const PeakReport = struct {
        host_peak: u64,
        backend_peak: u64,
        fits_host: bool,
        fits_backend: bool,
    };

    limits: TrainingBudgetLimits,
    host_by_category: [4]u64,
    backend_by_category: [4]u64,
    admit_count: u64 = 0,
    deny_count: u64 = 0,

    pub fn init(limits: TrainingBudgetLimits) TrainingBudget {
        return .{
            .limits = limits,
            .host_by_category = .{ 0, 0, 0, 0 },
            .backend_by_category = .{ 0, 0, 0, 0 },
        };
    }

    fn sumArr(arr: [4]u64) u64 {
        var total: u64 = 0;
        for (arr) |v| total += v;
        return total;
    }

    fn hostCeiling(self: *const TrainingBudget) u64 {
        const limit = self.limits.host_bytes;
        const reserved = self.limits.scratch_headroom_bytes +| self.limits.optimizer_reserve_bytes;
        if (reserved >= limit) return 0;
        return limit - reserved;
    }

    fn backendCeiling(self: *const TrainingBudget) u64 {
        const limit = self.limits.backend_bytes;
        // Scratch/optim are primarily host-side; backend ceiling only subtracts
        // the optimizer reserve if the user configured it (it may live on GPU).
        const reserved = self.limits.optimizer_reserve_bytes;
        if (reserved >= limit) return 0;
        return limit - reserved;
    }

    fn hostRemaining(self: *const TrainingBudget) u64 {
        const used = sumArr(self.host_by_category);
        const ceiling = self.hostCeiling();
        if (used >= ceiling) return 0;
        return ceiling - used;
    }

    fn backendRemaining(self: *const TrainingBudget) u64 {
        const used = sumArr(self.backend_by_category);
        const ceiling = self.backendCeiling();
        if (used >= ceiling) return 0;
        return ceiling - used;
    }

    pub fn tryReserve(
        self: *TrainingBudget,
        category: BudgetCategory,
        tier: Tier,
        bytes: u64,
    ) ReservationResult {
        const idx = @intFromEnum(category);
        switch (tier) {
            .host => {
                const new_used = sumArr(self.host_by_category) +| bytes;
                if (new_used > self.hostCeiling()) {
                    self.deny_count += 1;
                    return .{
                        .event = .denied_host_oom,
                        .reserved = 0,
                        .host_remaining = self.hostRemaining(),
                        .backend_remaining = self.backendRemaining(),
                    };
                }
                self.host_by_category[idx] += bytes;
            },
            .backend => {
                const new_used = sumArr(self.backend_by_category) +| bytes;
                if (new_used > self.backendCeiling()) {
                    self.deny_count += 1;
                    return .{
                        .event = .denied_backend_oom,
                        .reserved = 0,
                        .host_remaining = self.hostRemaining(),
                        .backend_remaining = self.backendRemaining(),
                    };
                }
                self.backend_by_category[idx] += bytes;
            },
        }
        self.admit_count += 1;
        return .{
            .event = .admitted,
            .reserved = bytes,
            .host_remaining = self.hostRemaining(),
            .backend_remaining = self.backendRemaining(),
        };
    }

    pub fn release(
        self: *TrainingBudget,
        category: BudgetCategory,
        tier: Tier,
        bytes: u64,
    ) void {
        if (bytes == 0) return;
        const idx = @intFromEnum(category);
        switch (tier) {
            .host => {
                const cur = self.host_by_category[idx];
                self.host_by_category[idx] = if (bytes >= cur) 0 else cur - bytes;
            },
            .backend => {
                const cur = self.backend_by_category[idx];
                self.backend_by_category[idx] = if (bytes >= cur) 0 else cur - bytes;
            },
        }
    }

    pub fn estimatePeak(
        self: *const TrainingBudget,
        estimates: []const PeakEstimate,
    ) PeakReport {
        var host_add: u64 = 0;
        var backend_add: u64 = 0;
        for (estimates) |est| {
            switch (est.tier) {
                .host => host_add +|= est.bytes,
                .backend => backend_add +|= est.bytes,
            }
        }
        const host_peak = sumArr(self.host_by_category) +| host_add;
        const backend_peak = sumArr(self.backend_by_category) +| backend_add;
        return .{
            .host_peak = host_peak,
            .backend_peak = backend_peak,
            .fits_host = host_peak <= self.hostCeiling(),
            .fits_backend = backend_peak <= self.backendCeiling(),
        };
    }

    pub fn format(self: *const TrainingBudget, out: []u8) ![]u8 {
        const w = @intFromEnum(BudgetCategory.weights);
        const g = @intFromEnum(BudgetCategory.gradients);
        const a = @intFromEnum(BudgetCategory.activations);
        const o = @intFromEnum(BudgetCategory.optimizer);
        return std.fmt.bufPrint(
            out,
            "host: {d}/{d} bytes ({d} weights, {d} grads, {d} activ, {d} optim), " ++
                "backend: {d}/{d} bytes ({d} weights, {d} grads, {d} activ, {d} optim)",
            .{
                sumArr(self.host_by_category),
                self.limits.host_bytes,
                self.host_by_category[w],
                self.host_by_category[g],
                self.host_by_category[a],
                self.host_by_category[o],
                sumArr(self.backend_by_category),
                self.limits.backend_bytes,
                self.backend_by_category[w],
                self.backend_by_category[g],
                self.backend_by_category[a],
                self.backend_by_category[o],
            },
        );
    }
};

test "fresh reserve weights on host" {
    var b = TrainingBudget.init(.{ .host_bytes = 16 * 1024 * 1024, .scratch_headroom_bytes = 0 });
    const r = b.tryReserve(.weights, .host, 1024 * 1024);
    try std.testing.expectEqual(BudgetEvent.admitted, r.event);
    try std.testing.expectEqual(@as(u64, 1024 * 1024), r.reserved);
    try std.testing.expectEqual(@as(u64, 15 * 1024 * 1024), r.host_remaining);
    try std.testing.expectEqual(@as(u64, 1), b.admit_count);
}

test "reserve and release reverses state" {
    var b = TrainingBudget.init(.{ .host_bytes = 8 * 1024 * 1024, .scratch_headroom_bytes = 0 });
    _ = b.tryReserve(.gradients, .host, 2 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 2 * 1024 * 1024), b.host_by_category[@intFromEnum(BudgetCategory.gradients)]);
    b.release(.gradients, .host, 2 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 0), b.host_by_category[@intFromEnum(BudgetCategory.gradients)]);
}

test "exceeding host_bytes denies and leaves state unchanged" {
    var b = TrainingBudget.init(.{ .host_bytes = 4 * 1024 * 1024, .scratch_headroom_bytes = 0 });
    _ = b.tryReserve(.weights, .host, 3 * 1024 * 1024);
    const before_used = b.host_by_category[@intFromEnum(BudgetCategory.weights)];
    const r = b.tryReserve(.activations, .host, 2 * 1024 * 1024);
    try std.testing.expectEqual(BudgetEvent.denied_host_oom, r.event);
    try std.testing.expectEqual(@as(u64, 0), r.reserved);
    try std.testing.expectEqual(before_used, b.host_by_category[@intFromEnum(BudgetCategory.weights)]);
    try std.testing.expectEqual(@as(u64, 0), b.host_by_category[@intFromEnum(BudgetCategory.activations)]);
    try std.testing.expectEqual(@as(u64, 1), b.deny_count);
}

test "scratch headroom is respected at the boundary" {
    var b = TrainingBudget.init(.{
        .host_bytes = 10 * 1024 * 1024,
        .scratch_headroom_bytes = 2 * 1024 * 1024,
    });
    // Available = 10 MiB - 2 MiB headroom = 8 MiB.
    const ok = b.tryReserve(.weights, .host, 8 * 1024 * 1024);
    try std.testing.expectEqual(BudgetEvent.admitted, ok.event);
    const denied = b.tryReserve(.activations, .host, 1);
    try std.testing.expectEqual(BudgetEvent.denied_host_oom, denied.event);
}

test "estimatePeak across both tiers" {
    var b = TrainingBudget.init(.{
        .host_bytes = 100 * 1024 * 1024,
        .backend_bytes = 50 * 1024 * 1024,
        .scratch_headroom_bytes = 0,
    });
    _ = b.tryReserve(.weights, .host, 10 * 1024 * 1024);
    _ = b.tryReserve(.weights, .backend, 20 * 1024 * 1024);
    const report = b.estimatePeak(&[_]TrainingBudget.PeakEstimate{
        .{ .category = .activations, .tier = .host, .bytes = 5 * 1024 * 1024 },
        .{ .category = .gradients, .tier = .backend, .bytes = 15 * 1024 * 1024 },
    });
    try std.testing.expectEqual(@as(u64, 15 * 1024 * 1024), report.host_peak);
    try std.testing.expectEqual(@as(u64, 35 * 1024 * 1024), report.backend_peak);
    try std.testing.expect(report.fits_host);
    try std.testing.expect(report.fits_backend);

    // A too-large estimate should report !fits.
    const tight = b.estimatePeak(&[_]TrainingBudget.PeakEstimate{
        .{ .category = .activations, .tier = .backend, .bytes = 40 * 1024 * 1024 },
    });
    try std.testing.expect(!tight.fits_backend);
}

test "optimizer reserve is deducted up front" {
    var b = TrainingBudget.init(.{
        .host_bytes = 10 * 1024 * 1024,
        .scratch_headroom_bytes = 0,
        .optimizer_reserve_bytes = 4 * 1024 * 1024,
    });
    // Usable = 10 - 4 = 6 MiB.
    const ok = b.tryReserve(.weights, .host, 6 * 1024 * 1024);
    try std.testing.expectEqual(BudgetEvent.admitted, ok.event);
    const denied = b.tryReserve(.weights, .host, 1);
    try std.testing.expectEqual(BudgetEvent.denied_host_oom, denied.event);
}

test "format writes a readable summary" {
    var b = TrainingBudget.init(.{
        .host_bytes = 1024,
        .backend_bytes = 2048,
        .scratch_headroom_bytes = 0,
    });
    _ = b.tryReserve(.weights, .host, 256);
    _ = b.tryReserve(.gradients, .backend, 512);
    var buf: [512]u8 = undefined;
    const out = try b.format(&buf);
    try std.testing.expect(std.mem.indexOf(u8, out, "host: 256/1024") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "backend: 512/2048") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "256 weights") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "512 grads") != null);
}
