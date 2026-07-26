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

pub const EstimateError = error{
    InvalidModelConfig,
    ResourceLimitExceeded,
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
        return addSaturating(addSaturating(self.host_weight_bytes, self.host_kv_bytes), self.host_scratch_bytes);
    }

    pub fn backendTotalBytes(self: *const RunBudget) usize {
        return addSaturating(addSaturating(self.backend_weight_bytes, self.backend_kv_bytes), self.backend_scratch_bytes);
    }

    pub fn kvTotalBytes(self: *const RunBudget) usize {
        return addSaturating(self.host_kv_bytes, self.backend_kv_bytes);
    }

    pub fn scratchTotalBytes(self: *const RunBudget) usize {
        return addSaturating(self.host_scratch_bytes, self.backend_scratch_bytes);
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
            addSaturating(current_bytes, bytes),
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

        const current_host = self.hostTotalBytes();
        const current_backend = self.backendTotalBytes();
        const current_kv = self.kvTotalBytes();
        const current_scratch = self.scratchTotalBytes();
        const next_host = switch (reservation.tier) {
            .host => std.math.add(usize, current_host, reservation.bytes) catch {
                self.recordDenial(.host_total, reservation, current_host, std.math.maxInt(usize), self.limits.host_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_host,
        };
        const next_backend = switch (reservation.tier) {
            .backend => std.math.add(usize, current_backend, reservation.bytes) catch {
                self.recordDenial(.backend_total, reservation, current_backend, std.math.maxInt(usize), self.limits.backend_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_backend,
        };
        const next_kv = switch (reservation.kind) {
            .kv => std.math.add(usize, current_kv, reservation.bytes) catch {
                self.recordDenial(.kv_total, reservation, current_kv, std.math.maxInt(usize), self.limits.kv_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_kv,
        };
        const next_scratch = switch (reservation.kind) {
            .scratch => std.math.add(usize, current_scratch, reservation.bytes) catch {
                self.recordDenial(.scratch_total, reservation, current_scratch, std.math.maxInt(usize), self.limits.scratch_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_scratch,
        };
        const next_combined = std.math.add(usize, next_host, next_backend) catch {
            self.recordDenial(
                .combined_total,
                reservation,
                addSaturating(current_host, current_backend),
                std.math.maxInt(usize),
                self.limits.combined_limit_bytes,
            );
            return error.MemoryBudgetExceeded;
        };

        if (self.limits.host_limit_bytes != 0 and next_host > self.limits.host_limit_bytes) {
            self.recordDenial(.host_total, reservation, self.hostTotalBytes(), next_host, self.limits.host_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.backend_limit_bytes != 0 and next_backend > self.limits.backend_limit_bytes) {
            self.recordDenial(.backend_total, reservation, self.backendTotalBytes(), next_backend, self.limits.backend_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.combined_limit_bytes != 0 and next_combined > self.limits.combined_limit_bytes) {
            self.recordDenial(.combined_total, reservation, addSaturating(self.hostTotalBytes(), self.backendTotalBytes()), next_combined, self.limits.combined_limit_bytes);
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
        self.denials +|= 1;
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

/// Process-wide resource admission. `RunBudget` protects one execution; this
/// controller accounts for resident models and concurrent executions together.
pub const AdmissionAmounts = struct {
    host_weight_bytes: usize = 0,
    backend_weight_bytes: usize = 0,
    host_kv_bytes: usize = 0,
    backend_kv_bytes: usize = 0,
    host_scratch_bytes: usize = 0,
    backend_scratch_bytes: usize = 0,

    pub fn hostTotalBytes(self: @This()) usize {
        return addSaturating(addSaturating(self.host_weight_bytes, self.host_kv_bytes), self.host_scratch_bytes);
    }

    pub fn backendTotalBytes(self: @This()) usize {
        return addSaturating(addSaturating(self.backend_weight_bytes, self.backend_kv_bytes), self.backend_scratch_bytes);
    }

    pub fn kvTotalBytes(self: @This()) usize {
        return addSaturating(self.host_kv_bytes, self.backend_kv_bytes);
    }

    pub fn scratchTotalBytes(self: @This()) usize {
        return addSaturating(self.host_scratch_bytes, self.backend_scratch_bytes);
    }

    pub fn fromEstimate(estimate: Estimate) @This() {
        var amounts: @This() = .{};
        switch (estimate.kv_tier) {
            .disk => {},
            .host => amounts.host_kv_bytes = estimate.kv_bytes,
            .backend => amounts.backend_kv_bytes = estimate.kv_bytes,
        }
        switch (estimate.scratch_tier) {
            .disk => {},
            .host => amounts.host_scratch_bytes = estimate.scratch_bytes,
            .backend => amounts.backend_scratch_bytes = estimate.scratch_bytes,
        }
        return amounts;
    }

    pub fn merge(self: @This(), other: @This()) !@This() {
        return addAdmissionAmounts(self, other) orelse error.ResourceLimitExceeded;
    }

    fn hostTotalBytesChecked(self: @This()) !usize {
        return std.math.add(
            usize,
            try std.math.add(usize, self.host_weight_bytes, self.host_kv_bytes),
            self.host_scratch_bytes,
        );
    }

    fn backendTotalBytesChecked(self: @This()) !usize {
        return std.math.add(
            usize,
            try std.math.add(usize, self.backend_weight_bytes, self.backend_kv_bytes),
            self.backend_scratch_bytes,
        );
    }

    fn kvTotalBytesChecked(self: @This()) !usize {
        return std.math.add(usize, self.host_kv_bytes, self.backend_kv_bytes);
    }

    fn scratchTotalBytesChecked(self: @This()) !usize {
        return std.math.add(usize, self.host_scratch_bytes, self.backend_scratch_bytes);
    }
};

pub const AdmissionLease = struct {
    controller: ?*AdmissionController,
    amounts: AdmissionAmounts,

    pub fn release(self: *AdmissionLease) void {
        const controller = self.controller orelse return;
        controller.release(self.amounts);
        self.controller = null;
        self.amounts = .{};
    }
};

pub const AdmissionController = struct {
    mutex: std.atomic.Mutex = .unlocked,
    admitted: AdmissionAmounts = .{},

    pub fn tryAcquire(
        self: *AdmissionController,
        limits: Limits,
        amounts: AdmissionAmounts,
        check_live_memory: bool,
    ) !AdmissionLease {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();

        const next = addAdmissionAmounts(self.admitted, amounts) orelse
            return error.ResourceLimitExceeded;
        const request_host = amounts.hostTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_backend = amounts.backendTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_combined = std.math.add(usize, request_host, request_backend) catch
            return error.ResourceLimitExceeded;
        const request_kv = amounts.kvTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_scratch = amounts.scratchTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_host = next.hostTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_backend = next.backendTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_combined = std.math.add(usize, next_host, next_backend) catch
            return error.ResourceLimitExceeded;
        const next_kv = next.kvTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_scratch = next.scratchTotalBytesChecked() catch return error.ResourceLimitExceeded;

        try checkAdmissionLimit(request_host, next_host, limits.host_limit_bytes);
        try checkAdmissionLimit(request_backend, next_backend, limits.backend_limit_bytes);
        try checkAdmissionLimit(request_combined, next_combined, limits.combined_limit_bytes);
        try checkAdmissionLimit(request_kv, next_kv, limits.kv_limit_bytes);
        try checkAdmissionLimit(request_scratch, next_scratch, limits.scratch_limit_bytes);

        if (check_live_memory) {
            // Metal allocations consume unified system memory. CUDA allocations are
            // accounted against the backend budget and must not also be charged to
            // Linux MemAvailable, or a valid device-resident model is rejected merely
            // because its VRAM footprint exceeds free host RAM.
            const live_host_incremental = if (builtin.os.tag == .macos)
                request_combined
            else
                request_host;
            try checkLiveHostMemory(live_host_incremental);
        }
        self.admitted = next;
        return .{ .controller = self, .amounts = amounts };
    }

    pub fn snapshot(self: *AdmissionController) AdmissionAmounts {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        return self.admitted;
    }

    fn release(self: *AdmissionController, amounts: AdmissionAmounts) void {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        self.admitted.host_weight_bytes -|= amounts.host_weight_bytes;
        self.admitted.backend_weight_bytes -|= amounts.backend_weight_bytes;
        self.admitted.host_kv_bytes -|= amounts.host_kv_bytes;
        self.admitted.backend_kv_bytes -|= amounts.backend_kv_bytes;
        self.admitted.host_scratch_bytes -|= amounts.host_scratch_bytes;
        self.admitted.backend_scratch_bytes -|= amounts.backend_scratch_bytes;
    }
};

fn spinLockAdmission(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn addAdmissionAmounts(a: AdmissionAmounts, b: AdmissionAmounts) ?AdmissionAmounts {
    return .{
        .host_weight_bytes = std.math.add(usize, a.host_weight_bytes, b.host_weight_bytes) catch return null,
        .backend_weight_bytes = std.math.add(usize, a.backend_weight_bytes, b.backend_weight_bytes) catch return null,
        .host_kv_bytes = std.math.add(usize, a.host_kv_bytes, b.host_kv_bytes) catch return null,
        .backend_kv_bytes = std.math.add(usize, a.backend_kv_bytes, b.backend_kv_bytes) catch return null,
        .host_scratch_bytes = std.math.add(usize, a.host_scratch_bytes, b.host_scratch_bytes) catch return null,
        .backend_scratch_bytes = std.math.add(usize, a.backend_scratch_bytes, b.backend_scratch_bytes) catch return null,
    };
}

fn addSaturating(a: usize, b: usize) usize {
    return std.math.add(usize, a, b) catch std.math.maxInt(usize);
}

fn checkAdmissionLimit(request: usize, next: usize, limit: usize) !void {
    if (limit == 0 or next <= limit) return;
    if (request > limit) return error.ResourceLimitExceeded;
    return error.ResourceTemporarilyUnavailable;
}

fn checkLiveHostMemory(incremental_bytes: usize) !void {
    const info = currentSystemMemoryInfo() orelse return;
    const available = info.available_bytes orelse return;
    const headroom = clampBytes(@max(info.total_bytes / 4, gib(6)), gib(4), gib(24));
    const required = std.math.add(usize, incremental_bytes, headroom) catch
        return error.ResourceLimitExceeded;
    if (available < required) return error.ResourceTemporarilyUnavailable;
}

pub fn defaultLimitsForBackend(backend: BackendClass) Limits {
    if (currentSystemMemoryInfo()) |info| {
        // Admission policy is stable for the lifetime of a machine configuration.
        // Current pressure is checked separately immediately before allocation.
        return deriveLimitsForBackend(backend, .{
            .total_bytes = info.total_bytes,
            .available_bytes = info.total_bytes,
        });
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
        .linux => probeSystemMemoryInfoLinux(),
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

const LinuxMemInfoFields = struct {
    total_kib: u64 = 0,
    available_kib: ?u64 = null,
    free_kib: u64 = 0,
    buffers_kib: u64 = 0,
    cached_kib: u64 = 0,
    reclaimable_kib: u64 = 0,
    shmem_kib: u64 = 0,
};

const CgroupMemoryInfo = struct {
    limit_bytes: ?usize = null,
    current_bytes: ?usize = null,
};

const CgroupPaths = struct {
    v2: ?[]const u8 = null,
    v1_memory: ?[]const u8 = null,
};

fn readSmallLinuxFile(path: []const u8, buffer: []u8) ?[]const u8 {
    if (builtin.os.tag != .linux or buffer.len == 0) return null;
    const fd = std.posix.openat(
        std.posix.AT.FDCWD,
        path,
        .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
        0,
    ) catch return null;
    defer _ = std.posix.system.close(fd);

    var used: usize = 0;
    while (used < buffer.len) {
        const count = std.posix.read(fd, buffer[used..]) catch return null;
        if (count == 0) break;
        used += count;
    }
    if (used == 0) return null;
    return buffer[0..used];
}

fn parseLinuxMemInfo(bytes: []const u8) ?SystemMemoryInfo {
    var fields = LinuxMemInfoFields{};
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const key = line[0..colon];
        var values = std.mem.tokenizeAny(u8, line[colon + 1 ..], " \t");
        const raw = values.next() orelse continue;
        const value = std.fmt.parseUnsigned(u64, raw, 10) catch continue;
        if (std.mem.eql(u8, key, "MemTotal")) {
            fields.total_kib = value;
        } else if (std.mem.eql(u8, key, "MemAvailable")) {
            fields.available_kib = value;
        } else if (std.mem.eql(u8, key, "MemFree")) {
            fields.free_kib = value;
        } else if (std.mem.eql(u8, key, "Buffers")) {
            fields.buffers_kib = value;
        } else if (std.mem.eql(u8, key, "Cached")) {
            fields.cached_kib = value;
        } else if (std.mem.eql(u8, key, "SReclaimable")) {
            fields.reclaimable_kib = value;
        } else if (std.mem.eql(u8, key, "Shmem")) {
            fields.shmem_kib = value;
        }
    }
    if (fields.total_kib == 0) return null;

    const available_kib = fields.available_kib orelse blk: {
        var fallback = std.math.add(u64, fields.free_kib, fields.buffers_kib) catch
            std.math.maxInt(u64);
        fallback = std.math.add(u64, fallback, fields.cached_kib) catch
            std.math.maxInt(u64);
        fallback = std.math.add(u64, fallback, fields.reclaimable_kib) catch
            std.math.maxInt(u64);
        break :blk fallback -| fields.shmem_kib;
    };
    const total_bytes_u64 = std.math.mul(u64, fields.total_kib, 1024) catch return null;
    const available_bytes_u64 = std.math.mul(u64, available_kib, 1024) catch
        std.math.maxInt(u64);
    return .{
        .total_bytes = std.math.cast(usize, total_bytes_u64) orelse return null,
        .available_bytes = std.math.cast(
            usize,
            @min(available_bytes_u64, total_bytes_u64),
        ),
    };
}

fn controllerListContains(controllers: []const u8, expected: []const u8) bool {
    var it = std.mem.splitScalar(u8, controllers, ',');
    while (it.next()) |controller| {
        if (std.mem.eql(u8, controller, expected)) return true;
    }
    return false;
}

fn parseCgroupPaths(bytes: []const u8) CgroupPaths {
    var result = CgroupPaths{};
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        const first = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const second = std.mem.indexOfScalarPos(u8, line, first + 1, ':') orelse continue;
        const hierarchy = line[0..first];
        const controllers = line[first + 1 .. second];
        const path = line[second + 1 ..];
        if (path.len == 0 or path[0] != '/' or std.mem.indexOf(u8, path, "..") != null) continue;
        if (std.mem.eql(u8, hierarchy, "0") and controllers.len == 0) {
            result.v2 = path;
        } else if (controllerListContains(controllers, "memory")) {
            result.v1_memory = path;
        }
    }
    return result;
}

fn cgroupFilePath(
    buffer: []u8,
    root: []const u8,
    relative: []const u8,
    filename: []const u8,
) ?[]const u8 {
    if (relative.len == 0 or relative[0] != '/') return null;
    return if (std.mem.eql(u8, relative, "/"))
        std.fmt.bufPrint(buffer, "{s}/{s}", .{ root, filename }) catch null
    else
        std.fmt.bufPrint(buffer, "{s}{s}/{s}", .{ root, relative, filename }) catch null;
}

fn readLinuxUnsignedFile(path: []const u8) ?usize {
    var buffer: [128]u8 = undefined;
    const bytes = readSmallLinuxFile(path, &buffer) orelse return null;
    const raw = std.mem.trim(u8, bytes, " \t\r\n");
    if (raw.len == 0 or std.mem.eql(u8, raw, "max")) return null;
    return std.fmt.parseUnsigned(usize, raw, 10) catch null;
}

fn readCgroupPair(
    root: []const u8,
    relative: []const u8,
    limit_filename: []const u8,
    current_filename: []const u8,
) CgroupMemoryInfo {
    var limit_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    var current_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const limit_path = cgroupFilePath(
        &limit_path_buffer,
        root,
        relative,
        limit_filename,
    ) orelse return .{};
    const current_path = cgroupFilePath(
        &current_path_buffer,
        root,
        relative,
        current_filename,
    ) orelse return .{};
    return .{
        .limit_bytes = readLinuxUnsignedFile(limit_path),
        .current_bytes = readLinuxUnsignedFile(current_path),
    };
}

fn probeCgroupMemoryInfoLinux() CgroupMemoryInfo {
    var cgroup_buffer: [4096]u8 = undefined;
    const cgroup_bytes = readSmallLinuxFile("/proc/self/cgroup", &cgroup_buffer) orelse
        return .{};
    const paths = parseCgroupPaths(cgroup_bytes);
    if (paths.v2) |path| {
        const info = readCgroupPair(
            "/sys/fs/cgroup",
            path,
            "memory.max",
            "memory.current",
        );
        if (info.limit_bytes != null) return info;
    }
    if (paths.v1_memory) |path| {
        const info = readCgroupPair(
            "/sys/fs/cgroup/memory",
            path,
            "memory.limit_in_bytes",
            "memory.usage_in_bytes",
        );
        if (info.limit_bytes != null) return info;
    }

    // Cgroup namespaces commonly expose the process cgroup as the mount root.
    var fallback = CgroupMemoryInfo{
        .limit_bytes = readLinuxUnsignedFile("/sys/fs/cgroup/memory.max"),
        .current_bytes = readLinuxUnsignedFile("/sys/fs/cgroup/memory.current"),
    };
    if (fallback.limit_bytes != null) return fallback;
    fallback = .{
        .limit_bytes = readLinuxUnsignedFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
        .current_bytes = readLinuxUnsignedFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    };
    return fallback;
}

fn applyCgroupMemoryInfo(
    host: SystemMemoryInfo,
    cgroup: CgroupMemoryInfo,
) SystemMemoryInfo {
    const raw_limit = cgroup.limit_bytes orelse return host;
    // Cgroup v1 represents "unlimited" with a very large numeric sentinel.
    // Ignore any limit above host RAM; applying memory.current to that sentinel
    // would incorrectly subtract process usage from MemAvailable a second time.
    if (raw_limit > host.total_bytes) return host;
    const limit = raw_limit;
    var available = host.available_bytes;
    if (cgroup.current_bytes) |current| {
        const cgroup_available = limit -| @min(current, limit);
        available = if (available) |host_available|
            @min(host_available, cgroup_available)
        else
            cgroup_available;
    }
    return .{
        .total_bytes = limit,
        .available_bytes = if (available) |value| @min(value, limit) else null,
    };
}

fn probeSystemMemoryInfoLinux() ?SystemMemoryInfo {
    if (builtin.os.tag != .linux) return null;
    var meminfo_buffer: [8192]u8 = undefined;
    const bytes = readSmallLinuxFile("/proc/meminfo", &meminfo_buffer) orelse return null;
    const host = parseLinuxMemInfo(bytes) orelse return null;
    return applyCgroupMemoryInfo(host, probeCgroupMemoryInfoLinux());
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
) EstimateError!Estimate {
    if (config.num_hidden_layers == 0 or
        config.hidden_size == 0 or
        config.num_attention_heads == 0 or
        config.vocab_size == 0)
    {
        return error.InvalidModelConfig;
    }
    const total_tokens = std.math.add(usize, prompt_tokens, max_tokens) catch
        return error.ResourceLimitExceeded;
    const retained_tokens = blk: {
        if (config.position_encoding != .absolute and config.sliding_window > 0) {
            break :blk @min(total_tokens, @as(usize, @intCast(config.sliding_window)));
        }
        if (config.position_encoding != .absolute and config.max_position_embeddings > 0) {
            break :blk @min(total_tokens, @as(usize, @intCast(config.max_position_embeddings)));
        }
        break :blk total_tokens;
    };
    const page_aligned_tokens = alignForwardChecked(@max(retained_tokens, 1), 16) catch
        return error.ResourceLimitExceeded;
    const max_kv_heads = config.maxKvHeads();
    const max_head_dim = try estimateMaxHeadDim(config);
    if (max_kv_heads == 0 or max_head_dim == 0) return error.InvalidModelConfig;
    const kv_pair_bytes = kv_dtype.bytesForTokenPairChecked(max_kv_heads, max_head_dim) catch
        return error.ResourceLimitExceeded;
    const token_layers = std.math.mul(
        usize,
        page_aligned_tokens,
        @as(usize, config.num_hidden_layers),
    ) catch return error.ResourceLimitExceeded;
    const kv_bytes = std.math.mul(usize, token_layers, kv_pair_bytes) catch
        return error.ResourceLimitExceeded;

    const scratch_rows = @max(prefill_chunk_size, 1);
    const hidden = @as(usize, @intCast(config.hidden_size));
    const heads = @as(usize, @intCast(config.num_attention_heads));
    const head_dim = @as(usize, @intCast(config.headDim()));
    const vocab = @as(usize, @intCast(config.vocab_size));
    const attention_width = std.math.mul(usize, heads, head_dim) catch
        return error.ResourceLimitExceeded;
    const hidden_scratch = checkedProduct(&.{ scratch_rows, hidden, 8, @sizeOf(f32) }) catch
        return error.ResourceLimitExceeded;
    const attn_scratch = checkedProduct(&.{ scratch_rows, @max(attention_width, hidden), 4, @sizeOf(f32) }) catch
        return error.ResourceLimitExceeded;
    const logits_scratch = std.math.mul(usize, vocab, @sizeOf(f32)) catch
        return error.ResourceLimitExceeded;
    const activation_scratch = std.math.add(usize, hidden_scratch, attn_scratch) catch
        return error.ResourceLimitExceeded;
    const scratch_bytes = std.math.add(usize, activation_scratch, logits_scratch) catch
        return error.ResourceLimitExceeded;

    return .{
        .prompt_tokens = prompt_tokens,
        .retained_tokens = retained_tokens,
        .kv_bytes = kv_bytes,
        .kv_tier = switch (backend) {
            .native => .host,
            .metal, .cuda => .backend,
        },
        .scratch_bytes = scratch_bytes,
        .scratch_tier = switch (backend) {
            .native => .host,
            .metal, .cuda => .backend,
        },
    };
}

fn alignForwardChecked(value: usize, alignment: usize) !usize {
    std.debug.assert(std.math.isPowerOfTwo(alignment));
    return (try std.math.add(usize, value, alignment - 1)) & ~(alignment - 1);
}

fn estimateMaxHeadDim(config: gpt_mod.Config) EstimateError!u32 {
    if (config.family != .deepseek_v4) return config.maxHeadDim();

    const base_head_dim = if (config.attention_head_dim > 0)
        config.attention_head_dim
    else
        config.hidden_size / config.num_attention_heads;
    const kv_lora: usize = if (config.deepseek_v4_kv_lora_rank > 0)
        config.deepseek_v4_kv_lora_rank
    else if (base_head_dim > config.deepseek_v4_qk_rope_head_dim)
        base_head_dim - config.deepseek_v4_qk_rope_head_dim
    else
        0;
    const width = std.math.add(usize, kv_lora, config.deepseek_v4_qk_rope_head_dim) catch
        return error.ResourceLimitExceeded;
    if (width > 0) {
        return std.math.cast(u32, width) orelse error.ResourceLimitExceeded;
    }
    const fallback = std.math.mul(
        usize,
        @as(usize, config.effectiveKVHeads()),
        @as(usize, base_head_dim),
    ) catch return error.ResourceLimitExceeded;
    return std.math.cast(u32, fallback) orelse error.ResourceLimitExceeded;
}

fn checkedProduct(values: []const usize) !usize {
    var result: usize = 1;
    for (values) |value| result = try std.math.mul(usize, result, value);
    return result;
}

test "linux meminfo parser uses MemAvailable and converts KiB" {
    const info = parseLinuxMemInfo(
        \\MemTotal:       16777216 kB
        \\MemFree:         1048576 kB
        \\MemAvailable:    4194304 kB
        \\Buffers:          131072 kB
        \\Cached:          2097152 kB
        \\
    ) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(gib(16), info.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(4)), info.available_bytes);
}

test "linux meminfo parser has a conservative legacy availability fallback" {
    const info = parseLinuxMemInfo(
        \\MemTotal:        8388608 kB
        \\MemFree:          524288 kB
        \\Buffers:          131072 kB
        \\Cached:          1048576 kB
        \\SReclaimable:     262144 kB
        \\Shmem:            131072 kB
        \\
    ) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(gib(8), info.total_bytes);
    try std.testing.expectEqual(@as(?usize, 1792 * 1024 * 1024), info.available_bytes);
}

test "cgroup paths and limits constrain host memory" {
    const paths = parseCgroupPaths(
        \\0::/system.slice/antfly.service
        \\7:cpu,cpuacct:/system.slice/antfly.service
        \\6:memory:/production/antfly
        \\
    );
    try std.testing.expectEqualStrings("/system.slice/antfly.service", paths.v2.?);
    try std.testing.expectEqualStrings("/production/antfly", paths.v1_memory.?);

    const effective = applyCgroupMemoryInfo(
        .{ .total_bytes = gib(64), .available_bytes = gib(32) },
        .{ .limit_bytes = gib(16), .current_bytes = gib(12) },
    );
    try std.testing.expectEqual(gib(16), effective.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(4)), effective.available_bytes);

    const v1_unlimited = applyCgroupMemoryInfo(
        .{ .total_bytes = gib(64), .available_bytes = gib(32) },
        .{ .limit_bytes = std.math.maxInt(usize) - 4095, .current_bytes = gib(12) },
    );
    try std.testing.expectEqual(gib(64), v1_unlimited.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(32)), v1_unlimited.available_bytes);
}

test "shared admission accounts for concurrent leases and releases capacity" {
    var controller = AdmissionController{};
    const limits = Limits{
        .host_limit_bytes = 100,
        .combined_limit_bytes = 100,
        .kv_limit_bytes = 100,
        .scratch_limit_bytes = 100,
    };
    var first = try controller.tryAcquire(limits, .{ .host_weight_bytes = 60 }, false);
    defer first.release();
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(limits, .{ .host_kv_bytes = 50 }, false),
    );
    first.release();
    var second = try controller.tryAcquire(limits, .{ .host_kv_bytes = 50 }, false);
    defer second.release();
    try std.testing.expectEqual(@as(usize, 50), controller.snapshot().hostTotalBytes());
}

test "single admission larger than policy is a resource limit" {
    var controller = AdmissionController{};
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquire(.{ .host_limit_bytes = 100 }, .{ .host_weight_bytes = 101 }, false),
    );
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

    const estimate = try estimateGptGeneration(.metal, .f16, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 110), estimate.retained_tokens);
    try std.testing.expectEqual(@as(usize, 112), estimate.kv_bytes / (32 * 8 * 128 * 2 * 2));
    try std.testing.expectEqual(ResidencyTier.backend, estimate.kv_tier);
    try std.testing.expect(estimate.scratch_bytes > 0);

    // int8: bytesForTokenRow(8, 128) = 1024 + 8*4 = 1056
    const est_int8 = try estimateGptGeneration(.metal, .int8, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 112 * 32 * 1056 * 2), est_int8.kv_bytes);

    // int4: bytesForTokenRow(8, 128) = ceil(1024/32)*18 = 32*18 = 576
    const est_int4 = try estimateGptGeneration(.metal, .int4, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 112 * 32 * 576 * 2), est_int4.kv_bytes);
}

test "gpt generation estimate rejects malformed and overflowing inputs" {
    var cfg = gpt_mod.Config{
        .hidden_size = 4096,
        .num_hidden_layers = 32,
        .num_attention_heads = 32,
        .num_key_value_heads = 8,
        .attention_head_dim = 128,
        .vocab_size = 32000,
    };
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        estimateGptGeneration(.native, .f16, cfg, std.math.maxInt(usize), 1, 256),
    );

    cfg.num_attention_heads = 0;
    try std.testing.expectError(
        error.InvalidModelConfig,
        estimateGptGeneration(.native, .f16, cfg, 1, 1, 256),
    );

    cfg.num_attention_heads = 32;
    cfg.family = .deepseek_v4;
    cfg.deepseek_v4_kv_lora_rank = std.math.maxInt(u32);
    cfg.deepseek_v4_qk_rope_head_dim = std.math.maxInt(u32);
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        estimateGptGeneration(.native, .f16, cfg, 1, 1, 256),
    );
}

test "admission rejects aggregate total overflow" {
    var controller = AdmissionController{};
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquire(
            .{},
            .{ .host_weight_bytes = std.math.maxInt(usize), .host_kv_bytes = 1 },
            false,
        ),
    );
}

test "combined target and draft estimates are admitted atomically" {
    const target = AdmissionAmounts.fromEstimate(.{
        .prompt_tokens = 8,
        .retained_tokens = 16,
        .kv_bytes = 40,
        .kv_tier = .host,
        .scratch_bytes = 10,
        .scratch_tier = .host,
    });
    const draft = AdmissionAmounts.fromEstimate(.{
        .prompt_tokens = 8,
        .retained_tokens = 16,
        .kv_bytes = 30,
        .kv_tier = .backend,
        .scratch_bytes = 20,
        .scratch_tier = .backend,
    });
    const combined = try target.merge(draft);
    try std.testing.expectEqual(@as(usize, 50), combined.hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 50), combined.backendTotalBytes());

    var controller = AdmissionController{};
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquire(.{ .combined_limit_bytes = 90 }, combined, false),
    );
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
}

test "run budget rejects accounting overflow even without configured limits" {
    var budget = RunBudget.init(.{});
    _ = try budget.tryReserveWeight(.host, std.math.maxInt(usize));
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.host, 1));
    try std.testing.expectEqual(DenialLimit.host_total, budget.last_denial.?.limit);
    try std.testing.expectEqual(std.math.maxInt(usize), budget.hostTotalBytes());
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
