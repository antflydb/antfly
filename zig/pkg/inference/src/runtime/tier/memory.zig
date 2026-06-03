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
const builtin = @import("builtin");
const planner = @import("planner.zig");
const kv_pool = @import("../kv/pool.zig");
const gpt_mod = @import("../../models/gpt.zig");

const macos = if (builtin.os.tag == .macos) struct {
    pub const kern_return_t = c_int;
    pub const integer_t = c_int;
    pub const natural_t = c_uint;
    pub const mach_msg_type_number_t = natural_t;
    pub const mach_port_t = c_uint;
    pub const host_t = mach_port_t;
    pub const host_flavor_t = integer_t;
    pub const vm_size_t = usize;
    pub const host_info64_t = [*]integer_t;

    pub const KERN_SUCCESS: kern_return_t = 0;
    pub const HOST_VM_INFO64: host_flavor_t = 4;

    // Avoid @cImport("mach/mach.h") here because Zig 0.16-dev can mis-translate
    // some generated Mach bindings on macOS. The probe only needs this narrow ABI.
    pub const vm_statistics64_data_t = extern struct {
        free_count: natural_t,
        active_count: natural_t,
        inactive_count: natural_t,
        wire_count: natural_t,
        zero_fill_count: u64,
        reactivations: u64,
        pageins: u64,
        pageouts: u64,
        faults: u64,
        cow_faults: u64,
        lookups: u64,
        hits: u64,
        purges: u64,
        purgeable_count: natural_t,
        speculative_count: natural_t,
        decompressions: u64,
        compressions: u64,
        swapins: u64,
        swapouts: u64,
        compressor_page_count: natural_t,
        throttled_count: natural_t,
        external_page_count: natural_t,
        internal_page_count: natural_t,
        total_uncompressed_pages_in_compressor: u64,
    };

    pub const HOST_VM_INFO64_COUNT: mach_msg_type_number_t =
        @as(mach_msg_type_number_t, @intCast(@sizeOf(vm_statistics64_data_t) / @sizeOf(integer_t)));

    pub extern fn sysctlbyname(
        name: [*:0]const u8,
        oldp: ?*anyopaque,
        oldlenp: *usize,
        newp: ?*anyopaque,
        newlen: usize,
    ) c_int;
    pub extern fn mach_host_self() mach_port_t;
    pub extern fn host_page_size(host: host_t, out_page_size: *vm_size_t) kern_return_t;
    pub extern fn host_statistics64(
        host: host_t,
        flavor: host_flavor_t,
        host_info_out: host_info64_t,
        host_info_out_cnt: *mach_msg_type_number_t,
    ) kern_return_t;
} else struct {};

pub const ResidencyTier = planner.ResidencyTier;
pub const BackendClass = planner.BackendClass;

pub const Limits = struct {
    host_limit_bytes: usize = 0,
    backend_limit_bytes: usize = 0,
    combined_limit_bytes: usize = 0,
    kv_limit_bytes: usize = 0,
    scratch_limit_bytes: usize = 0,
};

pub const SystemMemoryInfo = struct {
    total_bytes: usize,
    available_bytes: ?usize = null,
};

pub const ReservationKind = enum {
    weight,
    kv,
    scratch,
};

pub const Reservation = struct {
    kind: ReservationKind,
    tier: ResidencyTier,
    bytes: usize,
};

pub const DenialLimit = enum {
    host_total,
    backend_total,
    combined_total,
    kv_total,
    scratch_total,
    shared_cache_host,
    shared_cache_backend,
};

pub const Denial = struct {
    reservation: Reservation,
    limit: DenialLimit,
    current_bytes: usize,
    requested_total_bytes: usize,
    limit_bytes: usize,
    host_total_bytes: usize,
    backend_total_bytes: usize,
    kv_total_bytes: usize,
    scratch_total_bytes: usize,
};

pub const Estimate = struct {
    prompt_tokens: usize,
    retained_tokens: usize,
    kv_bytes: usize,
    kv_tier: ResidencyTier,
    scratch_bytes: usize,
    scratch_tier: ResidencyTier,
};

pub const RunBudget = struct {
    limits: Limits,
    host_weight_bytes: usize = 0,
    backend_weight_bytes: usize = 0,
    host_kv_bytes: usize = 0,
    backend_kv_bytes: usize = 0,
    host_scratch_bytes: usize = 0,
    backend_scratch_bytes: usize = 0,
    denials: u64 = 0,
    last_denial: ?Denial = null,
    peak_host_total_bytes: usize = 0,
    peak_backend_total_bytes: usize = 0,

    pub fn init(limits: Limits) RunBudget {
        return .{ .limits = limits };
    }

    pub fn reserveEstimate(self: *RunBudget, estimate: Estimate) !void {
        try self.tryReserve(.{ .kind = .kv, .tier = estimate.kv_tier, .bytes = estimate.kv_bytes });
        errdefer self.release(.{ .kind = .kv, .tier = estimate.kv_tier, .bytes = estimate.kv_bytes });
        try self.tryReserve(.{ .kind = .scratch, .tier = estimate.scratch_tier, .bytes = estimate.scratch_bytes });
    }

    pub fn tryReserveWeight(self: *RunBudget, tier: ResidencyTier, bytes: usize) !Reservation {
        const reservation = Reservation{
            .kind = .weight,
            .tier = tier,
            .bytes = bytes,
        };
        try self.tryReserve(reservation);
        return reservation;
    }

    pub fn release(self: *RunBudget, reservation: Reservation) void {
        if (reservation.bytes == 0 or reservation.tier == .disk) return;
        switch (reservation.kind) {
            .weight => switch (reservation.tier) {
                .disk => {},
                .host => self.host_weight_bytes -|= reservation.bytes,
                .backend => self.backend_weight_bytes -|= reservation.bytes,
            },
            .kv => switch (reservation.tier) {
                .disk => {},
                .host => self.host_kv_bytes -|= reservation.bytes,
                .backend => self.backend_kv_bytes -|= reservation.bytes,
            },
            .scratch => switch (reservation.tier) {
                .disk => {},
                .host => self.host_scratch_bytes -|= reservation.bytes,
                .backend => self.backend_scratch_bytes -|= reservation.bytes,
            },
        }
    }

    pub fn hostTotalBytes(self: *const RunBudget) usize {
        return self.host_weight_bytes + self.host_kv_bytes + self.host_scratch_bytes;
    }

    pub fn backendTotalBytes(self: *const RunBudget) usize {
        return self.backend_weight_bytes + self.backend_kv_bytes + self.backend_scratch_bytes;
    }

    pub fn kvTotalBytes(self: *const RunBudget) usize {
        return self.host_kv_bytes + self.backend_kv_bytes;
    }

    pub fn scratchTotalBytes(self: *const RunBudget) usize {
        return self.host_scratch_bytes + self.backend_scratch_bytes;
    }

    pub fn noteSharedCacheDenial(
        self: *RunBudget,
        tier: ResidencyTier,
        bytes: usize,
        current_bytes: usize,
        limit_bytes: usize,
    ) void {
        if (tier == .disk) return;
        self.recordDenial(
            switch (tier) {
                .disk => unreachable,
                .host => .shared_cache_host,
                .backend => .shared_cache_backend,
            },
            .{ .kind = .weight, .tier = tier, .bytes = bytes },
            current_bytes,
            current_bytes + bytes,
            limit_bytes,
        );
    }

    pub fn hasLastDenial(self: *const RunBudget) bool {
        return self.last_denial != null;
    }

    pub fn lastDenialString(self: *const RunBudget, buf: []u8) ![]const u8 {
        const denial = self.last_denial orelse {
            return std.fmt.bufPrint(buf, "memory budget exceeded", .{});
        };
        return std.fmt.bufPrint(
            buf,
            "memory budget exceeded: limit={s} reservation={s}/{s} current={d} request={d} next={d} limit={d} totals(host={d} backend={d} kv={d} scratch={d})",
            .{
                @tagName(denial.limit),
                @tagName(denial.reservation.kind),
                @tagName(denial.reservation.tier),
                denial.current_bytes,
                denial.reservation.bytes,
                denial.requested_total_bytes,
                denial.limit_bytes,
                denial.host_total_bytes,
                denial.backend_total_bytes,
                denial.kv_total_bytes,
                denial.scratch_total_bytes,
            },
        );
    }

    fn tryReserve(self: *RunBudget, reservation: Reservation) !void {
        if (reservation.bytes == 0 or reservation.tier == .disk) return;

        const next_host = switch (reservation.tier) {
            .host => self.hostTotalBytes() + reservation.bytes,
            else => self.hostTotalBytes(),
        };
        const next_backend = switch (reservation.tier) {
            .backend => self.backendTotalBytes() + reservation.bytes,
            else => self.backendTotalBytes(),
        };
        const next_kv = switch (reservation.kind) {
            .kv => self.kvTotalBytes() + reservation.bytes,
            else => self.kvTotalBytes(),
        };
        const next_scratch = switch (reservation.kind) {
            .scratch => self.scratchTotalBytes() + reservation.bytes,
            else => self.scratchTotalBytes(),
        };
        const next_combined = next_host + next_backend;

        if (self.limits.host_limit_bytes != 0 and next_host > self.limits.host_limit_bytes) {
            self.recordDenial(.host_total, reservation, self.hostTotalBytes(), next_host, self.limits.host_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.backend_limit_bytes != 0 and next_backend > self.limits.backend_limit_bytes) {
            self.recordDenial(.backend_total, reservation, self.backendTotalBytes(), next_backend, self.limits.backend_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.combined_limit_bytes != 0 and next_combined > self.limits.combined_limit_bytes) {
            self.recordDenial(.combined_total, reservation, self.hostTotalBytes() + self.backendTotalBytes(), next_combined, self.limits.combined_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.kv_limit_bytes != 0 and next_kv > self.limits.kv_limit_bytes) {
            self.recordDenial(.kv_total, reservation, self.kvTotalBytes(), next_kv, self.limits.kv_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.scratch_limit_bytes != 0 and next_scratch > self.limits.scratch_limit_bytes) {
            self.recordDenial(.scratch_total, reservation, self.scratchTotalBytes(), next_scratch, self.limits.scratch_limit_bytes);
            return error.MemoryBudgetExceeded;
        }

        switch (reservation.kind) {
            .weight => switch (reservation.tier) {
                .disk => {},
                .host => self.host_weight_bytes += reservation.bytes,
                .backend => self.backend_weight_bytes += reservation.bytes,
            },
            .kv => switch (reservation.tier) {
                .disk => {},
                .host => self.host_kv_bytes += reservation.bytes,
                .backend => self.backend_kv_bytes += reservation.bytes,
            },
            .scratch => switch (reservation.tier) {
                .disk => {},
                .host => self.host_scratch_bytes += reservation.bytes,
                .backend => self.backend_scratch_bytes += reservation.bytes,
            },
        }

        self.peak_host_total_bytes = @max(self.peak_host_total_bytes, self.hostTotalBytes());
        self.peak_backend_total_bytes = @max(self.peak_backend_total_bytes, self.backendTotalBytes());
    }

    fn recordDenial(
        self: *RunBudget,
        limit: DenialLimit,
        reservation: Reservation,
        current_bytes: usize,
        requested_total_bytes: usize,
        limit_bytes: usize,
    ) void {
        self.denials += 1;
        self.last_denial = .{
            .reservation = reservation,
            .limit = limit,
            .current_bytes = current_bytes,
            .requested_total_bytes = requested_total_bytes,
            .limit_bytes = limit_bytes,
            .host_total_bytes = self.hostTotalBytes(),
            .backend_total_bytes = self.backendTotalBytes(),
            .kv_total_bytes = self.kvTotalBytes(),
            .scratch_total_bytes = self.scratchTotalBytes(),
        };
    }
};

pub fn defaultLimitsForBackend(backend: BackendClass) Limits {
    if (currentSystemMemoryInfo()) |info| {
        return deriveLimitsForBackend(backend, info);
    }
    return staticLimitsForBackend(backend);
}

fn staticLimitsForBackend(backend: BackendClass) Limits {
    return switch (backend) {
        .cpu => .{
            .host_limit_bytes = 2 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 0,
            .combined_limit_bytes = 2 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 768 * 1024 * 1024,
            .scratch_limit_bytes = 256 * 1024 * 1024,
        },
        .gpu => .{
            .host_limit_bytes = 2 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 6 * 1024 * 1024 * 1024,
            .combined_limit_bytes = 8 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 1024 * 1024 * 1024,
            .scratch_limit_bytes = 512 * 1024 * 1024,
        },
    };
}

pub fn currentSystemMemoryInfo() ?SystemMemoryInfo {
    return switch (builtin.os.tag) {
        .macos => probeSystemMemoryInfoMacos(),
        else => null,
    };
}

fn deriveLimitsForBackend(backend: BackendClass, info: SystemMemoryInfo) Limits {
    const total = info.total_bytes;
    const available = info.available_bytes orelse total;
    const reserve_headroom = clampBytes(@max(total / 4, gib(6)), gib(4), gib(24));
    const safe_pool = available -| @min(available, reserve_headroom);
    const usable = @max(safe_pool, gib(2));

    return switch (backend) {
        .cpu => .{
            .host_limit_bytes = clampBytes(usable / 2, gib(2), gib(8)),
            .backend_limit_bytes = 0,
            .combined_limit_bytes = clampBytes(usable / 2, gib(2), gib(8)),
            .kv_limit_bytes = clampBytes(usable / 6, mib(512), gib(2)),
            .scratch_limit_bytes = clampBytes(usable / 12, mib(256), gib(1)),
        },
        .gpu => blk: {
            const combined = clampBytes(usable / 2, gib(6), gib(12));
            break :blk .{
                .host_limit_bytes = clampBytes(combined / 4, gib(1), gib(3)),
                .backend_limit_bytes = clampBytes((combined * 3) / 4, gib(4), gib(9)),
                .combined_limit_bytes = combined,
                .kv_limit_bytes = clampBytes(combined / 4, mib(768), gib(3)),
                .scratch_limit_bytes = clampBytes(combined / 8, mib(384), gib(2)),
            };
        },
    };
}

fn probeSystemMemoryInfoMacos() ?SystemMemoryInfo {
    if (builtin.os.tag != .macos) return null;

    var total_raw: u64 = 0;
    var total_len: usize = @sizeOf(u64);
    if (macos.sysctlbyname("hw.memsize", @ptrCast(&total_raw), &total_len, null, 0) != 0 or total_raw == 0) return null;

    var page_size: macos.vm_size_t = 0;
    if (macos.host_page_size(macos.mach_host_self(), &page_size) != macos.KERN_SUCCESS or page_size == 0) {
        return .{ .total_bytes = @intCast(total_raw), .available_bytes = null };
    }

    var vm_stats: macos.vm_statistics64_data_t = undefined;
    var count: macos.mach_msg_type_number_t = macos.HOST_VM_INFO64_COUNT;
    if (macos.host_statistics64(
        macos.mach_host_self(),
        macos.HOST_VM_INFO64,
        @ptrCast(&vm_stats),
        &count,
    ) != macos.KERN_SUCCESS) {
        return .{ .total_bytes = @intCast(total_raw), .available_bytes = null };
    }

    const available_pages: u64 =
        @as(u64, @intCast(vm_stats.free_count)) +
        @as(u64, @intCast(vm_stats.inactive_count)) +
        @as(u64, @intCast(vm_stats.speculative_count));
    const available_bytes_u64 = available_pages * @as(u64, @intCast(page_size));
    return .{
        .total_bytes = @intCast(total_raw),
        .available_bytes = @intCast(@min(available_bytes_u64, total_raw)),
    };
}

fn mib(value: usize) usize {
    return value * 1024 * 1024;
}

fn gib(value: usize) usize {
    return value * 1024 * 1024 * 1024;
}

fn clampBytes(value: usize, min_value: usize, max_value: usize) usize {
    return @min(@max(value, min_value), max_value);
}

pub fn estimateGptGeneration(
    backend: kv_pool.BackendKind,
    kv_dtype: kv_pool.KvDType,
    config: gpt_mod.Config,
    prompt_tokens: usize,
    max_tokens: usize,
    prefill_chunk_size: usize,
) Estimate {
    const total_tokens = prompt_tokens + max_tokens;
    const retained_tokens = blk: {
        if (config.position_encoding != .absolute and config.sliding_window > 0) {
            break :blk @min(total_tokens, @as(usize, @intCast(config.sliding_window)));
        }
        if (config.position_encoding != .absolute and config.max_position_embeddings > 0) {
            break :blk @min(total_tokens, @as(usize, @intCast(config.max_position_embeddings)));
        }
        break :blk total_tokens;
    };
    const page_aligned_tokens = std.mem.alignForward(usize, @max(retained_tokens, 1), 16);
    const kv_pair_bytes = kv_dtype.bytesForTokenPair(
        @intCast(config.maxKvHeads()),
        @intCast(config.maxHeadDim()),
    );
    const kv_bytes = page_aligned_tokens * @as(usize, @intCast(config.num_hidden_layers)) * kv_pair_bytes;

    const scratch_rows = @max(prefill_chunk_size, 1);
    const hidden = @as(usize, @intCast(config.hidden_size));
    const heads = @as(usize, @intCast(config.num_attention_heads));
    const head_dim = @as(usize, @intCast(config.headDim()));
    const vocab = @as(usize, @intCast(config.vocab_size));
    const hidden_scratch = scratch_rows * hidden * @as(usize, 8) * @sizeOf(f32);
    const attn_scratch = scratch_rows * @max(heads * head_dim, hidden) * @as(usize, 4) * @sizeOf(f32);
    const logits_scratch = vocab * @sizeOf(f32);
    const scratch_bytes = hidden_scratch + attn_scratch + logits_scratch;

    return .{
        .prompt_tokens = prompt_tokens,
        .retained_tokens = retained_tokens,
        .kv_bytes = kv_bytes,
        .kv_tier = switch (backend) {
            .native => .host,
            .metal, .mlx, .cuda => .backend,
        },
        .scratch_bytes = scratch_bytes,
        .scratch_tier = switch (backend) {
            .native => .host,
            .metal, .mlx, .cuda => .backend,
        },
    };
}

test "run budget enforces kv and scratch separately from host total" {
    var budget = RunBudget.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 80,
        .combined_limit_bytes = 140,
        .kv_limit_bytes = 40,
        .scratch_limit_bytes = 20,
    });

    try budget.reserveEstimate(.{
        .prompt_tokens = 4,
        .retained_tokens = 8,
        .kv_bytes = 30,
        .kv_tier = .host,
        .scratch_bytes = 10,
        .scratch_tier = .host,
    });
    try std.testing.expectEqual(@as(usize, 40), budget.hostTotalBytes());
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.host, 70));
    try std.testing.expect(budget.hasLastDenial());
    try std.testing.expectEqual(DenialLimit.host_total, budget.last_denial.?.limit);
    const reservation = try budget.tryReserveWeight(.host, 20);
    try std.testing.expectEqual(@as(usize, 60), budget.hostTotalBytes());
    budget.release(reservation);
    try std.testing.expectEqual(@as(usize, 40), budget.hostTotalBytes());
}

test "run budget formats denial details" {
    var budget = RunBudget.init(.{
        .host_limit_bytes = 64,
        .backend_limit_bytes = 0,
        .combined_limit_bytes = 64,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.host, 80));

    var buf: [256]u8 = undefined;
    const msg = try budget.lastDenialString(&buf);
    try std.testing.expect(std.mem.indexOf(u8, msg, "host_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "weight/host") != null);
}

test "run budget enforces combined host and backend total" {
    var budget = RunBudget.init(.{
        .host_limit_bytes = 0,
        .backend_limit_bytes = 0,
        .combined_limit_bytes = 100,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });

    _ = try budget.tryReserveWeight(.host, 60);
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.backend, 50));
    try std.testing.expectEqual(DenialLimit.combined_total, budget.last_denial.?.limit);
}

test "gpt generation estimate accounts for sliding window and page alignment" {
    const cfg = gpt_mod.Config{
        .hidden_size = 4096,
        .num_hidden_layers = 32,
        .num_attention_heads = 32,
        .num_key_value_heads = 8,
        .attention_head_dim = 128,
        .vocab_size = 32000,
        .sliding_window = 4096,
        .position_encoding = .rope,
    };

    const estimate = estimateGptGeneration(.mlx, .f16, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 110), estimate.retained_tokens);
    try std.testing.expectEqual(@as(usize, 112), estimate.kv_bytes / (32 * 8 * 128 * 2 * 2));
    try std.testing.expectEqual(ResidencyTier.backend, estimate.kv_tier);
    try std.testing.expect(estimate.scratch_bytes > 0);

    // int8: bytesForTokenRow(8, 128) = 1024 + 8*4 = 1056
    const est_int8 = estimateGptGeneration(.mlx, .int8, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 112 * 32 * 1056 * 2), est_int8.kv_bytes);

    // int4: bytesForTokenRow(8, 128) = ceil(1024/32)*18 = 32*18 = 576
    const est_int4 = estimateGptGeneration(.mlx, .int4, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 112 * 32 * 576 * 2), est_int4.kv_bytes);
}

test "derive gpu limits keeps combined cap sane" {
    const limits = deriveLimitsForBackend(.gpu, .{
        .total_bytes = gib(64),
        .available_bytes = gib(40),
    });
    try std.testing.expect(limits.combined_limit_bytes >= gib(6));
    try std.testing.expect(limits.combined_limit_bytes <= gib(12));
    try std.testing.expect(limits.backend_limit_bytes <= limits.combined_limit_bytes);
}
